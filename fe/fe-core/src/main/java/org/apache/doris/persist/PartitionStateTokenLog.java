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

package org.apache.doris.persist;

import org.apache.doris.catalog.PartitionStateToken;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

/**
 * The state tokens given to partitions of a table outside of a transaction (a backup names the partitions that have
 * none), see PartitionStateToken. The tokens have a random part, so they are logged and not made again on replay.
 */
@Getter
public class PartitionStateTokenLog implements Writable {

    @SerializedName(value = "db")
    private long dbId;

    @SerializedName(value = "tb")
    private long tableId;

    // partition id -> the state token it gets
    @SerializedName(value = "tks")
    private Map<Long, PartitionStateToken> tokens;

    public PartitionStateTokenLog(long dbId, long tableId, Map<Long, PartitionStateToken> tokens) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.tokens = tokens;
    }

    public static PartitionStateTokenLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, PartitionStateTokenLog.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }
}
