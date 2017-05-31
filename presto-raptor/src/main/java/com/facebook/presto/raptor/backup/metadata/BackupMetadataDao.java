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

import com.facebook.presto.raptor.util.UuidUtil.UuidArgumentFactory;
import com.facebook.presto.raptor.util.UuidUtil.UuidMapperFactory;
import org.skife.jdbi.v2.sqlobject.Bind;
//import org.skife.jdbi.v2.sqlobject.GetGeneratedKeys;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterArgumentFactory;
import org.skife.jdbi.v2.sqlobject.customizers.RegisterMapperFactory;

import java.sql.Timestamp;

@RegisterArgumentFactory(UuidArgumentFactory.class)
@RegisterMapperFactory(UuidMapperFactory.class)
public interface BackupMetadataDao
{
    @SqlUpdate("INSERT INTO backup_centera (\n" +
            " shard_uuid, clipid, filename, creation_poolid,\n" +
            " modification_poolid, retention_period, retention_class, Type,\n" +
            " Name, creation_date, modification_date, creation_profile,\n" +
            " modification_profile, numfiles, totalsize, naming_scheme,\n" +
            " numtags, app_vendor, app_name, app_version)\n" +
            " VALUES (\n" +
            " :shardUuid, :clipId, :fileName, :creationPoolid,\n" +
            " :modificationPoolid, :retentionPeriod, :retentionClass, :type,\n" +
            " :name, :creationDate, :modificationDate, :creationProfile,\n" +
            " :modificationProfile, :numFiles, :totalSize, :namingScheme,\n" +
            " :numTags, :appVendor, :appName, :appVersion)")
    //@GetGeneratedKeys
    void insertCenteraClipInfoForShard(
            @Bind("shardUuid") String shardUuid, @Bind("clipId") String clipId,
            @Bind("fileName") String fileName, @Bind("creationPoolid") String creationPoolid,
            @Bind("modificationPoolid") String modificationPoolid,
            @Bind("retentionPeriod") Long retentionPeriod,  @Bind("retentionClass") String retentionClass,
            @Bind("type") String type, @Bind("name") String name, @Bind("creationDate") Timestamp creationDate,
            @Bind("modificationDate") Timestamp modificationDate, @Bind("creationProfile") String creationProfile,
            @Bind("modificationProfile") String modificationProfile, @Bind("numFiles") int numFiles,
            @Bind("totalSize") Long totalSize, @Bind("namingScheme") String namingScheme,
            @Bind("numTags") int numTags, @Bind("appVendor") String appVendor,
            @Bind("appName") String appName, @Bind("appVersion") String appVersion);

    @SqlQuery("SELECT clipid FROM backup_centera WHERE shard_uuid = :shardUuid")
    String getCenteraClipIdForShard(@Bind("shardUuid") String shardUuid);

    @SqlUpdate("DELETE FROM backup_centera \n" +
               "WHERE shard_uuid = :shardUuid")
    void deleteCenteraClipInfoForShard(@Bind("shardUuid") String shardUuid);
}
