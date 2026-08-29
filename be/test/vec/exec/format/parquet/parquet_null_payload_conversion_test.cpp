// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include <cstring>
#include <vector>

#include "runtime/types.h"
#include "util/slice.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exec/format/column_type_convert.h"
#include "vec/exec/format/parquet/fix_length_plain_decoder.h"
#include "vec/exec/format/parquet/parquet_column_convert.h"
#include "vec/exec/format/parquet/schema_desc.h"

namespace doris::vectorized {

using Decimal128Column = ColumnDecimal<Decimal128V3>;

// Regression tests for NULL payload handling in the parquet reading chain:
//   1. fixed-width decoders must initialize nested payload of retained NULL
//      logical slots to zero, instead of leaving stale bytes from a reused
//      buffer;
//   2. the physical-to-logical converter must locate the current batch from
//      the end of the accumulated (cross-batch) destination null map;
//   3. the shared logical type converter must use the accumulated destination
//      offset so NULL rows are skipped and never validated;
//   4. non-NULL rows must keep the strict decimal conversion semantics.
//
// All tests use deterministic poison values, so they do not depend on
// allocator fill patterns, ASAN behavior or execution order. Only public
// interfaces are used.
class ParquetNullPayloadConversionTest : public ::testing::Test {
protected:
    static constexpr int kTypeLength = 16;
    static constexpr int kSrcPrecision = 38;
    static constexpr int kSrcScale = 4;
    static constexpr int kDstPrecision = 26;
    static constexpr int kPrefixRows = 4;
    static constexpr int kBatchRows = 4;

    static Int128 valid_value_1() { return 123456; } // 12.3456 with scale 4
    static Int128 valid_value_2() { return -78901; } // -7.8901 with scale 4

    // Fits into DECIMAL(38,4) but exceeds DECIMAL(26,4), so any non-NULL
    // conversion of it must fail the narrowing check.
    static Int128 poison_value() {
        Int128 value = 1;
        for (int i = 0; i < 30; ++i) {
            value *= 10;
        }
        return value;
    }

    static FieldSchema create_decimal_field_schema() {
        FieldSchema field_schema;
        field_schema.name = "amount";
        tparquet::SchemaElement& schema = field_schema.parquet_schema;
        schema.__set_type(tparquet::Type::FIXED_LEN_BYTE_ARRAY);
        schema.__set_type_length(kTypeLength);
        schema.__set_precision(kSrcPrecision);
        schema.__set_scale(kSrcScale);
        schema.__set_repetition_type(tparquet::FieldRepetitionType::OPTIONAL);
        field_schema.is_nullable = true;
        field_schema.physical_type = tparquet::Type::FIXED_LEN_BYTE_ARRAY;
        field_schema.type = TypeDescriptor(TYPE_DECIMAL128I);
        field_schema.type.precision = kSrcPrecision;
        field_schema.type.scale = kSrcScale;
        field_schema.definition_level = 1;
        field_schema.repetition_level = 0;
        return field_schema;
    }

    // Encodes an unscaled decimal as two's complement big-endian, which is
    // how parquet stores FIXED_LEN_BYTE_ARRAY decimals.
    static void encode_big_endian_decimal(Int128 value, uint8_t* out) {
        UInt128 raw;
        memcpy(&raw, &value, sizeof(raw));
        for (size_t i = 0; i < kTypeLength; ++i) {
            out[i] = static_cast<uint8_t>((raw >> ((kTypeLength - 1 - i) * 8)) & 0xff);
        }
    }

    static void assert_decimal_value(const Decimal128Column& column, size_t pos, Int128 expected,
                                     const char* what) {
        EXPECT_EQ(Decimal128V3(expected), column.get_data()[pos]) << what << " at row " << pos;
    }
};

// The decoder must zero the nested payload of NULL logical slots that are
// retained in the result, even when the output buffer is reused from a
// previous batch and still holds poison bytes.
TEST_F(ParquetNullPayloadConversionTest, FixedLengthPlainDecoderInitializesReusedNullSlots) {
    // Physical data only contains the two non-NULL values.
    uint8_t raw[kTypeLength * 2];
    encode_big_endian_decimal(valid_value_1(), raw);
    encode_big_endian_decimal(valid_value_2(), raw + kTypeLength);
    Slice data_slice(raw, sizeof(raw));

    FixLengthPlainDecoder decoder;
    decoder.set_type_length(kTypeLength);
    ASSERT_TRUE(decoder.set_data(&data_slice).ok());

    // Poison the reusable output buffer and clear it, keeping the capacity,
    // which is exactly the state of a cached column between batches.
    MutableColumnPtr column = ColumnUInt8::create();
    assert_cast<ColumnUInt8*>(column.get())->get_data().resize_fill(kTypeLength * kBatchRows, 0x7f);
    ASSERT_LE(kTypeLength * kBatchRows,
              assert_cast<const ColumnUInt8*>(column.get())->get_data().capacity());
    column->clear();
    ASSERT_EQ(0, column->size());

    DataTypePtr data_type = std::make_shared<DataTypeUInt8>();

    // Logical pattern: NULL, CONTENT, NULL, CONTENT. Run lengths alternate
    // starting with CONTENT, so a zero-length first run makes it start with
    // a NULL.
    size_t num_values = kBatchRows;
    std::vector<uint16_t> run_length_null_map = {0, 1, 1, 1, 1};
    std::vector<uint8_t> filter_data(num_values, 1);
    FilterMap filter_map;
    ASSERT_TRUE(filter_map.init(filter_data.data(), filter_data.size(), false).ok());
    ColumnSelectVector select_vector;
    NullMap null_map;
    ASSERT_TRUE(
            select_vector.init(run_length_null_map, num_values, &null_map, &filter_map, 0).ok());

    ASSERT_TRUE(decoder.decode_values(column, data_type, select_vector, false).ok());

    // Four logical rows are retained, each with a 16-byte payload.
    ASSERT_EQ(kTypeLength * kBatchRows, column->size());
    const auto& data = assert_cast<const ColumnUInt8*>(column.get())->get_data();

    // NULL rows must be fully zeroed instead of keeping the poison.
    for (size_t row : {0, 2}) {
        for (size_t i = 0; i < kTypeLength; ++i) {
            EXPECT_EQ(0, data[row * kTypeLength + i])
                    << "NULL row " << row << " byte " << i << " must be zero";
        }
    }

    // Non-NULL rows decode to the big-endian inputs.
    uint8_t expected[kTypeLength];
    encode_big_endian_decimal(valid_value_1(), expected);
    EXPECT_EQ(0, memcmp(expected, data.data() + kTypeLength, kTypeLength));
    encode_big_endian_decimal(valid_value_2(), expected);
    EXPECT_EQ(0, memcmp(expected, data.data() + 3 * kTypeLength, kTypeLength));

    // Null map of the current batch is [1, 0, 1, 0].
    ASSERT_EQ(kBatchRows, null_map.size());
    EXPECT_EQ(1, null_map[0]);
    EXPECT_EQ(0, null_map[1]);
    EXPECT_EQ(1, null_map[2]);
    EXPECT_EQ(0, null_map[3]);

    // The decoder must have consumed only the two non-NULL physical values,
    // so any further read runs out of bounds.
    MutableColumnPtr extra = ColumnUInt8::create();
    std::vector<uint16_t> one_more = {1};
    ColumnSelectVector extra_select;
    NullMap extra_null_map;
    ASSERT_TRUE(extra_select.init(one_more, 1, &extra_null_map, &filter_map, 0).ok());
    ASSERT_FALSE(decoder.decode_values(extra, data_type, extra_select, false).ok());
}

// The physical-to-logical converter must skip conversion of NULL rows by
// looking them up in the accumulated destination null map, using the current
// batch interval (offset by the prefix rows), not the batch-local index.
TEST_F(ParquetNullPayloadConversionTest, FixedSizeDecimalPhysicalConverterUsesCurrentBatchNullMap) {
    FieldSchema field_schema = create_decimal_field_schema();
    auto dst_logical_type =
            std::make_shared<DataTypeNullable>(create_decimal(kDstPrecision, kSrcScale, false));
    auto converter = parquet::PhysicalToLogicalConverter::get_converter(
            &field_schema, field_schema.type, dst_logical_type, nullptr, false);
    ASSERT_TRUE(converter->support());

    // Accumulated destination null map: 4 prefix rows are all non-NULL, the
    // current batch contributes [1, 0, 1, 0]. If the converter wrongly used
    // batch-local indices 0..3 instead of the accumulated interval 4..7, the
    // prefix flags would make every row non-NULL and the poison payloads
    // would be converted.
    auto null_map_column = ColumnUInt8::create();
    auto& null_map_data = null_map_column->get_data();
    for (uint8_t flag : {0, 0, 0, 0, 1, 0, 1, 0}) {
        null_map_data.push_back(flag);
    }
    auto nested = create_decimal(kSrcPrecision, kSrcScale, false)->create_column();
    auto src_logical_column = ColumnNullable::create(nested->get_ptr(), null_map_column->get_ptr());
    ASSERT_EQ(kPrefixRows + kBatchRows, src_logical_column->size());

    // Physical column: local rows 0 and 2 carry poison payloads for the NULL
    // rows, rows 1 and 3 carry valid big-endian decimals.
    auto physical_column = ColumnUInt8::create();
    auto& physical_data = physical_column->get_data();
    physical_data.resize(kTypeLength * kBatchRows);
    for (size_t row : {0, 2}) {
        for (size_t i = 0; i < kTypeLength; ++i) {
            physical_data[row * kTypeLength + i] = 0x7f;
        }
    }
    encode_big_endian_decimal(valid_value_1(),
                              physical_data.data() + kTypeLength); // NOLINT
    encode_big_endian_decimal(valid_value_2(),
                              physical_data.data() + 3 * kTypeLength); // NOLINT

    ColumnPtr src_physical_col = std::move(physical_column);
    ColumnPtr src_logical_col = std::move(src_logical_column);
    ASSERT_TRUE(converter->physical_convert(src_physical_col, src_logical_col).ok());

    const auto* converted =
            assert_cast<const Decimal128Column*>(remove_nullable(src_logical_col).get());
    ASSERT_EQ(kBatchRows, converted->size());
    // NULL rows get the default payload instead of the poison.
    assert_decimal_value(*converted, 0, 0, "physical NULL row must be default");
    assert_decimal_value(*converted, 2, 0, "physical NULL row must be default");
    // Non-NULL rows decode the unscaled values.
    assert_decimal_value(*converted, 1, valid_value_1(), "physical non-NULL row");
    assert_decimal_value(*converted, 3, valid_value_2(), "physical non-NULL row");

    // The shared accumulated null map must be left unchanged.
    const auto& final_null_map =
            assert_cast<const ColumnNullable*>(src_logical_col.get())->get_null_map_data();
    ASSERT_EQ(kPrefixRows + kBatchRows, final_null_map.size());
    for (size_t i = 0; i < kPrefixRows; ++i) {
        EXPECT_EQ(0, final_null_map[i]);
    }
    EXPECT_EQ(1, final_null_map[4]);
    EXPECT_EQ(0, final_null_map[5]);
    EXPECT_EQ(1, final_null_map[6]);
    EXPECT_EQ(0, final_null_map[7]);
}

// The shared logical converter must skip strict decimal conversion for NULL
// rows located via the accumulated destination offset. Poison values sitting
// at NULL positions must never reach the narrowing check.
TEST_F(ParquetNullPayloadConversionTest, DecimalLogicalConverterUsesAccumulatedDestinationOffset) {
    TypeDescriptor src_type = TypeDescriptor(TYPE_DECIMAL128I);
    src_type.precision = kSrcPrecision;
    src_type.scale = kSrcScale;
    auto dst_logical_type =
            std::make_shared<DataTypeNullable>(create_decimal(kDstPrecision, kSrcScale, false));
    auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_logical_type,
                                                                   converter::FileFormat::PARQUET);
    ASSERT_TRUE(converter->support());
    ASSERT_FALSE(converter->is_consistent());

    auto mutable_dst = dst_logical_type->create_column();
    auto& nullable_dst = assert_cast<ColumnNullable&>(*mutable_dst);
    auto& nested_dst = assert_cast<Decimal128Column&>(nullable_dst.get_nested_column());
    auto& null_map = nullable_dst.get_null_map_data();
    // 4 prefix rows, all non-NULL.
    for (int i = 0; i < kPrefixRows; ++i) {
        nested_dst.get_data().push_back(Decimal128V3((i + 1) * 100000));
        null_map.push_back(0);
    }
    // Current batch flags: [1, 0, 1, 0].
    for (uint8_t flag : {1, 0, 1, 0}) {
        null_map.push_back(flag);
    }

    // Batch-local source values with poison at the NULL positions.
    auto src_mutable = create_decimal(kSrcPrecision, kSrcScale, false)->create_column();
    auto& src_data = assert_cast<Decimal128Column&>(*src_mutable).get_data();
    src_data.push_back(Decimal128V3(poison_value()));
    src_data.push_back(Decimal128V3(valid_value_1()));
    src_data.push_back(Decimal128V3(poison_value()));
    src_data.push_back(Decimal128V3(valid_value_2()));

    ColumnPtr src_col = std::move(src_mutable);
    Status st = converter->convert(src_col, mutable_dst);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(kPrefixRows + kBatchRows, nested_dst.size());
    // Prefix rows keep their values.
    for (int i = 0; i < kPrefixRows; ++i) {
        assert_decimal_value(nested_dst, i, (i + 1) * 100000, "prefix row");
    }
    // Global rows 4 and 6 are NULL and must be default payload.
    assert_decimal_value(nested_dst, 4, 0, "logical NULL row must be default");
    assert_decimal_value(nested_dst, 6, 0, "logical NULL row must be default");
    // Global rows 5 and 7 are converted values.
    assert_decimal_value(nested_dst, 5, valid_value_1(), "logical non-NULL row");
    assert_decimal_value(nested_dst, 7, valid_value_2(), "logical non-NULL row");

    // The accumulated null map must stay [0,0,0,0,1,0,1,0].
    ASSERT_EQ(kPrefixRows + kBatchRows, null_map.size());
    EXPECT_EQ(1, null_map[4]);
    EXPECT_EQ(0, null_map[5]);
    EXPECT_EQ(1, null_map[6]);
    EXPECT_EQ(0, null_map[7]);
}

// A poison value at a non-NULL position must still fail the strict decimal
// narrowing check. This guards against "fixing" the NULL issue by silently
// truncating or skipping overflow rows.
TEST_F(ParquetNullPayloadConversionTest, NonNullDecimalOverflowStillFails) {
    TypeDescriptor src_type = TypeDescriptor(TYPE_DECIMAL128I);
    src_type.precision = kSrcPrecision;
    src_type.scale = kSrcScale;
    auto dst_logical_type =
            std::make_shared<DataTypeNullable>(create_decimal(kDstPrecision, kSrcScale, false));
    auto converter = converter::ColumnTypeConverter::get_converter(src_type, dst_logical_type,
                                                                   converter::FileFormat::PARQUET);
    ASSERT_TRUE(converter->support());

    auto mutable_dst = dst_logical_type->create_column();
    auto& nullable_dst = assert_cast<ColumnNullable&>(*mutable_dst);
    auto& nested_dst = assert_cast<Decimal128Column&>(nullable_dst.get_nested_column());
    auto& null_map = nullable_dst.get_null_map_data();
    // All prefix and batch rows are non-NULL.
    for (int i = 0; i < kPrefixRows; ++i) {
        nested_dst.get_data().push_back(Decimal128V3((i + 1) * 100000));
        null_map.push_back(0);
    }
    for (int i = 0; i < kBatchRows; ++i) {
        null_map.push_back(0);
    }

    // The first row of the batch carries the poison at a non-NULL position.
    auto src_mutable = create_decimal(kSrcPrecision, kSrcScale, false)->create_column();
    auto& src_data = assert_cast<Decimal128Column&>(*src_mutable).get_data();
    src_data.push_back(Decimal128V3(poison_value()));
    src_data.push_back(Decimal128V3(valid_value_1()));
    src_data.push_back(Decimal128V3(valid_value_2()));
    src_data.push_back(Decimal128V3(valid_value_1()));

    ColumnPtr src_col = std::move(src_mutable);
    Status st = converter->convert(src_col, mutable_dst);
    ASSERT_FALSE(st.ok());
    EXPECT_NE(std::string::npos, st.to_string().find("Failed to cast value")) << st.to_string();
}

} // namespace doris::vectorized
