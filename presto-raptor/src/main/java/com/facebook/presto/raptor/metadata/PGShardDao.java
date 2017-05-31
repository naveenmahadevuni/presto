/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.raptor.metadata;

import com.facebook.presto.raptor.util.UuidUtil.UuidArgumentFactory;
import com.facebook.presto.raptor.util.UuidUtil.UuidMapperFactory;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlBatch;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterArgumentFactory;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapperFactory;

import java.sql.Timestamp;
import java.util.Set;
import java.util.UUID;

@RegisterArgumentFactory(UuidArgumentFactory.class)
@RegisterMapperFactory(UuidMapperFactory.class)
public interface PGShardDao
        extends ShardDao
{
    @SqlQuery("SELECT table_id, shard_id, shard_uuid, bucket_number, row_count, compressed_size, uncompressed_size\n" +
            "FROM (\n" +
            "    SELECT s.*\n" +
            "    FROM shards s\n" +
            "    JOIN shard_nodes sn ON (s.shard_id = sn.shard_id)\n" +
            "    JOIN nodes n ON (sn.node_id = n.node_id)\n" +
            "    WHERE n.node_identifier = :nodeIdentifier\n" +
            "      AND s.bucket_number IS NULL\n" +
            "      AND (s.table_id = :tableId OR cast(:tableId as bigint) IS NULL)\n" +
            "  UNION ALL\n" +
            "    SELECT s.*\n" +
            "    FROM shards s\n" +
            "    JOIN tables t ON (s.table_id = t.table_id)\n" +
            "    JOIN distributions d ON (t.distribution_id = d.distribution_id)\n" +
            "    JOIN buckets b ON (\n" +
            "      d.distribution_id = b.distribution_id AND\n" +
            "      s.bucket_number = b.bucket_number)\n" +
            "    JOIN nodes n ON (b.node_id = n.node_id)\n" +
            "    WHERE n.node_identifier = :nodeIdentifier\n" +
            "      AND (s.table_id = :tableId OR cast(:tableId as bigint) IS NULL)\n" +
            ") x")
    @Mapper(ShardMetadata.Mapper.class)
    Set<ShardMetadata> getNodeShards(@Bind("nodeIdentifier") String nodeIdentifier, @Bind("tableId") Long tableId);

    @Override
    @SqlUpdate("DELETE \n" +
            "FROM shard_nodes sn \n" +
            "USING shards s WHERE sn.shard_id = s.shard_id\n" +
            "AND  table_id = :tableId")
    void dropShardNodes(@Bind("tableId") long tableId);

    @Override
    @SqlBatch("INSERT INTO deleted_shards (shard_uuid, delete_time)\n" +
            "VALUES (:shardUuid, CURRENT_TIMESTAMP)")
    void insertDeletedShards(@Bind("shardUuid") Iterable<UUID> shardUuids);

    @SqlUpdate("DELETE FROM transactions\n" +
            "WHERE transaction_id in (\n" +
            "SELECT transaction_id from transactions\n" +
            "WHERE end_time < :maxEndTime\n" +
            "  AND successful IN (TRUE, FALSE)\n" +
            "  AND transaction_id NOT IN (SELECT transaction_id FROM created_shards)\n" +
            "LIMIT " + CLEANUP_TRANSACTIONS_BATCH_SIZE + ")")
    int deleteOldCompletedTransactions(@Bind("maxEndTime") Timestamp maxEndTime);
}
