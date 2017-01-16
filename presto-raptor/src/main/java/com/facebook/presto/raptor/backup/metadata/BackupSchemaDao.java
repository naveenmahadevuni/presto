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
package com.facebook.presto.raptor.backup.metadata;

import org.skife.jdbi.v2.sqlobject.SqlUpdate;

public interface BackupSchemaDao
{
    @SqlUpdate("CREATE TABLE IF NOT EXISTS backup_centera (\n" +
            "  shard_uuid VARCHAR(36) NOT NULL,\n" +
            "  clipid VARCHAR(53),\n" +
            "  filename TEXT ,\n" +
            "  creation_poolid TEXT NOT NULL,\n" +
            "  modification_poolid TEXT ,\n" +
            "  retention_period BIGINT ,\n" +
            "  retention_class TEXT ,\n" +
            "  Type TEXT ,\n" +
            "  Name TEXT ,\n" +
            "  creation_date DATETIME NOT NULL ,\n" +
            "  modification_date DATETIME ,\n" +
            "  creation_profile TEXT NOT NULL,\n" +
            "  modification_profile TEXT ,\n" +
            "  numfiles INT NOT NULL ,\n" +
            "  totalsize BIGINT NOT NULL ,\n" +
            "  naming_scheme TEXT ,\n" +
            "  numtags INT NOT NULL ,\n" +
            "  app_vendor TEXT NOT NULL ,\n" +
            "  app_name TEXT NOT NULL ,\n" +
            "  app_version TEXT NOT NULL ,\n" +
            "  PRIMARY KEY (shard_uuid)\n" +
            ")")
    void createCenteraMetadataTable();
}
