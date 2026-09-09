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

#include "runtime/runtime_state.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exec/format/parquet/vparquet_column_reader.h"
#include "vec/exec/format/parquet/vparquet_group_reader.h"

namespace doris::vectorized {
namespace {

// Exercise the real dictionary-column replacement before an EOF-first predicate read.
class EofDictColumnReader final : public ParquetColumnReader {
public:
    explicit EofDictColumnReader(const std::vector<RowRange>& ranges)
            : ParquetColumnReader(ranges, nullptr, nullptr) {}
    Status read_column_data(ColumnPtr& column, DataTypePtr& type,
                            const std::shared_ptr<TableSchemaChangeHelper::Node>&, FilterMap&,
                            size_t, size_t* rows, bool* eof, bool is_dict_filter) override {
        EXPECT_TRUE(is_dict_filter);
        EXPECT_EQ(column->size(), 0);
        EXPECT_EQ(remove_nullable(type)->get_type_id(), TypeIndex::Int32);
        ++reads;
        *rows = 0;
        *eof = true;
        return Status::OK();
    }
    MutableColumnPtr convert_dict_column_to_string_column(const ColumnInt32* column) override {
        EXPECT_EQ(column->size(), 0);
        ++conversions;
        return ColumnString::create();
    }
    const std::vector<level_t>& get_rep_level() const override { return levels; }
    const std::vector<level_t>& get_def_level() const override { return levels; }
    Statistics statistics() override { return {}; }
    void close() override {}
    void reset_filter_map_index() override {}
    int reads = 0;
    int conversions = 0;
    std::vector<level_t> levels;
};

TEST(ParquetLazyEofTest, RestoresDictionaryColumnTypesAtFirstEof) {
    for (bool nullable : {false, true}) {
        SCOPED_TRACE(nullable);
        RuntimeState state((TQueryGlobals()));
        std::vector<std::string> columns = {"s", "payload"};
        std::vector<RowRange> ranges;
        tparquet::RowGroup row_group;
        RowGroupReader::PositionDeleteContext deletes(0, 0);
        RowGroupReader::LazyReadContext lazy;
        lazy.can_lazy_read = true;
        lazy.predicate_columns = {{"s"}, {0}};
        lazy.lazy_read_columns = {"payload"};
        RowGroupReader reader(nullptr, columns, 0, row_group, nullptr, nullptr, deletes, lazy,
                              &state);
        auto root = std::make_shared<TableSchemaChangeHelper::StructNode>();
        root->add_children("s", "s", std::make_shared<TableSchemaChangeHelper::ScalarNode>());
        reader._table_info_node_ptr = root;
        reader._dict_filter_cols.emplace_back("s", 0);
        auto predicate = std::make_unique<EofDictColumnReader>(ranges);
        auto* predicate_ptr = predicate.get();
        reader._column_readers.emplace("s", std::move(predicate));
        DataTypePtr type = std::make_shared<DataTypeString>();
        if (nullable) {
            type = make_nullable(type);
        }
        Block block;
        block.insert({type->create_column(), type, "s"});
        block.insert({type->create_column(), type, "payload"});
        size_t rows = 1;
        bool eof = false;
        ASSERT_TRUE(reader.next_batch(&block, 1024, &rows, &eof).ok());
        EXPECT_EQ(rows, 0);
        EXPECT_TRUE(eof);
        EXPECT_EQ(predicate_ptr->reads, 1);
        EXPECT_EQ(predicate_ptr->conversions, 1);
        const auto& result = block.get_by_name("s");
        EXPECT_TRUE(result.type->equals(*type));
        const IColumn* column = result.column.get();
        if (nullable) {
            const auto* nullable_column = check_and_get_column<ColumnNullable>(*column);
            ASSERT_NE(nullable_column, nullptr);
            EXPECT_EQ(nullable_column->get_null_map_column().size(), 0);
            column = &nullable_column->get_nested_column();
        }
        EXPECT_NE(check_and_get_column<ColumnString>(*column), nullptr);
    }
}

} // namespace
} // namespace doris::vectorized
