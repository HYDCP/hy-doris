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

suite("test_backup_restore_streaming_compatibility", "backup_restore,nonConcurrent") {
    String repoName = "test_backup_restore_streaming_compatibility_repo_" +
            UUID.randomUUID().toString().replace("-", "")
    def syncer = getSyncer()
    def oldStreaming = (sql """
        ADMIN SHOW FRONTEND CONFIG LIKE 'enable_table_meta_streaming_json'
    """)[0][1]
    def oldReserveReplica = (sql """
        ADMIN SHOW FRONTEND CONFIG LIKE 'backup_meta_reserve_replica_info'
    """)[0][1]
    boolean repoCreated = false

    try {
        syncer.createS3Repository(repoName)
        repoCreated = true

        sql "CREATE DATABASE IF NOT EXISTS test_backup_restore_streaming_compatibility_db"
        sql """
            DROP TABLE IF EXISTS test_backup_restore_streaming_compatibility_db.test_backup_restore_streaming_compatibility_table
        """
        sql """
            CREATE TABLE test_backup_restore_streaming_compatibility_db.test_backup_restore_streaming_compatibility_table (
                `id` LARGEINT NOT NULL,
                `value` LARGEINT SUM DEFAULT "0"
            )
            AGGREGATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES ("replication_num" = "1")
        """
        sql """
            INSERT INTO test_backup_restore_streaming_compatibility_db.test_backup_restore_streaming_compatibility_table
            VALUES (1, 10), (2, 20), (3, 30)
        """
        order_qt_initial """
            SELECT id, value
            FROM test_backup_restore_streaming_compatibility_db.test_backup_restore_streaming_compatibility_table
            ORDER BY id
        """

        // Legacy writer with Replica information retained. Switch to the streaming
        // reader before restore to prove cross-mode compatibility.
        sql "ADMIN SET FRONTEND CONFIG ('enable_table_meta_streaming_json' = 'false')"
        sql "ADMIN SET FRONTEND CONFIG ('backup_meta_reserve_replica_info' = 'true')"
        sql """
            BACKUP SNAPSHOT test_backup_restore_streaming_compatibility_db.test_backup_restore_streaming_compatibility_legacy_snapshot
            TO `${repoName}`
            ON (test_backup_restore_streaming_compatibility_table)
        """
        syncer.waitSnapshotFinish("test_backup_restore_streaming_compatibility_db")
        def legacySnapshot = syncer.getSnapshotTimestamp(
                repoName, "test_backup_restore_streaming_compatibility_legacy_snapshot")

        sql "ADMIN SET FRONTEND CONFIG ('enable_table_meta_streaming_json' = 'true')"
        sql """
            TRUNCATE TABLE test_backup_restore_streaming_compatibility_db.test_backup_restore_streaming_compatibility_table
        """
        sql """
            RESTORE SNAPSHOT test_backup_restore_streaming_compatibility_db.test_backup_restore_streaming_compatibility_legacy_snapshot
            FROM `${repoName}`
            ON (test_backup_restore_streaming_compatibility_table)
            PROPERTIES (
                "backup_timestamp" = "${legacySnapshot}",
                "reserve_replica" = "true"
            )
        """
        syncer.waitAllRestoreFinish("test_backup_restore_streaming_compatibility_db")
        order_qt_legacy_writer_streaming_reader """
            SELECT id, value
            FROM test_backup_restore_streaming_compatibility_db.test_backup_restore_streaming_compatibility_table
            ORDER BY id
        """

        // Streaming writer with Replica information stripped. Switch to the legacy
        // reader before restore to prove rollback compatibility.
        sql "ADMIN SET FRONTEND CONFIG ('enable_table_meta_streaming_json' = 'true')"
        sql "ADMIN SET FRONTEND CONFIG ('backup_meta_reserve_replica_info' = 'false')"
        sql """
            BACKUP SNAPSHOT test_backup_restore_streaming_compatibility_db.test_backup_restore_streaming_compatibility_streaming_snapshot
            TO `${repoName}`
            ON (test_backup_restore_streaming_compatibility_table)
        """
        syncer.waitSnapshotFinish("test_backup_restore_streaming_compatibility_db")
        def streamingSnapshot = syncer.getSnapshotTimestamp(
                repoName, "test_backup_restore_streaming_compatibility_streaming_snapshot")

        sql "ADMIN SET FRONTEND CONFIG ('enable_table_meta_streaming_json' = 'false')"
        sql """
            TRUNCATE TABLE test_backup_restore_streaming_compatibility_db.test_backup_restore_streaming_compatibility_table
        """
        sql """
            RESTORE SNAPSHOT test_backup_restore_streaming_compatibility_db.test_backup_restore_streaming_compatibility_streaming_snapshot
            FROM `${repoName}`
            ON (test_backup_restore_streaming_compatibility_table)
            PROPERTIES (
                "backup_timestamp" = "${streamingSnapshot}",
                "reserve_replica" = "true"
            )
        """
        syncer.waitAllRestoreFinish("test_backup_restore_streaming_compatibility_db")
        order_qt_streaming_writer_legacy_reader """
            SELECT id, value
            FROM test_backup_restore_streaming_compatibility_db.test_backup_restore_streaming_compatibility_table
            ORDER BY id
        """
    } finally {
        sql "ADMIN SET FRONTEND CONFIG ('enable_table_meta_streaming_json' = '${oldStreaming}')"
        sql "ADMIN SET FRONTEND CONFIG ('backup_meta_reserve_replica_info' = '${oldReserveReplica}')"
        if (repoCreated) {
            try_sql "DROP REPOSITORY `${repoName}`"
        }
    }
}
