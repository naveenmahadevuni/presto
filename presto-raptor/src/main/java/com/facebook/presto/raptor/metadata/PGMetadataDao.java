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

import com.facebook.presto.spi.SchemaTableName;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.customizers.Mapper;

import java.util.List;

public interface PGMetadataDao extends MetadataDao
{
    @SqlQuery("SELECT schema_name, table_name\n" +
            "FROM tables\n" +
            "WHERE (schema_name = :schemaName OR cast(:schemaName as VARCHAR(255)) IS NULL)")
    @Mapper(SchemaTableNameMapper.class)
    List<SchemaTableName> listTables(
            @Bind("schemaName") String schemaName);

    @SqlQuery(TABLE_COLUMN_SELECT +
            "WHERE (schema_name = :schemaName OR cast(:schemaName as VARCHAR(255)) IS NULL)\n" +
            "  AND (table_name = :tableName OR cast(:tableName as VARCHAR(255)) IS NULL)\n" +
            "ORDER BY schema_name, table_name, ordinal_position")
    List<TableColumn> listTableColumns(
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName);

    @SqlQuery("SELECT schema_name, table_name, data\n" +
            "FROM views\n" +
            "WHERE (schema_name = :schemaName OR cast(:schemaName as VARCHAR(255)) IS NULL)")
    @Mapper(SchemaTableNameMapper.class)
    List<SchemaTableName> listViews(
            @Bind("schemaName") String schemaName);

    @SqlQuery("SELECT schema_name, table_name, data\n" +
            "FROM views\n" +
            "WHERE (schema_name = :schemaName OR cast(:schemaName as VARCHAR(255)) IS NULL)\n" +
            "  AND (table_name = :tableName OR cast(:tableName as VARCHAR(255)) IS NULL)\n" +
            "ORDER BY schema_name, table_name\n")
    @Mapper(ViewResult.Mapper.class)
    List<ViewResult> getViews(
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName);

    @SqlQuery("SELECT table_id, schema_name, table_name, temporal_column_id, distribution_name, bucket_count, organization_enabled\n" +
            "FROM tables\n" +
            "LEFT JOIN distributions\n" +
            "ON tables.distribution_id = distributions.distribution_id\n" +
            "WHERE (schema_name = :schemaName OR cast(:schemaName as VARCHAR(255)) IS NULL)\n" +
            "  AND (table_name = :tableName OR cast(:tableName as VARCHAR(255)) IS NULL)\n" +
            "ORDER BY table_id")
    @Mapper(TableMetadataRow.Mapper.class)
    List<TableMetadataRow> getTableMetadataRows(
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName);

    @SqlQuery("SELECT table_id, column_id, column_name, sort_ordinal_position, bucket_ordinal_position\n" +
            "FROM columns\n" +
            "WHERE table_id IN (\n" +
            "  SELECT table_id\n" +
            "  FROM tables\n" +
            "  WHERE (schema_name = :schemaName OR cast(:schemaName as VARCHAR(255)) IS NULL)\n" +
            "    AND (table_name = :tableName OR cast(:tableName as VARCHAR(255)) IS NULL))\n" +
            "ORDER BY table_id")
    @Mapper(ColumnMetadataRow.Mapper.class)
    List<ColumnMetadataRow> getColumnMetadataRows(
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName);

    @SqlQuery("SELECT schema_name, table_name, create_time, update_time, table_version,\n" +
            "  shard_count, row_count, compressed_size, uncompressed_size\n" +
            "FROM tables\n" +
            "WHERE (schema_name = :schemaName OR cast(:schemaName as VARCHAR(255)) IS NULL)\n" +
            "  AND (table_name = :tableName OR cast(:tableName as VARCHAR(255)) IS NULL)\n" +
            "ORDER BY schema_name, table_name")
    @Mapper(TableStatsRow.Mapper.class)
    List<TableStatsRow> getTableStatsRows(
            @Bind("schemaName") String schemaName,
            @Bind("tableName") String tableName);
}
