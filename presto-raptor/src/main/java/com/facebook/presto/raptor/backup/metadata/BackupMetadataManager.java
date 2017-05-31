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

import com.facebook.presto.raptor.metadata.ForMetadata;
import com.facebook.presto.raptor.util.DaoSupplier;
import io.airlift.log.Logger;
import org.skife.jdbi.v2.IDBI;

import javax.inject.Inject;
import static com.facebook.presto.raptor.backup.metadata.BackupSchemaDaoUtil.createCenteraBackupMetadataTablesWithRetry;
import static java.util.Objects.requireNonNull;

public class BackupMetadataManager
{
    private static final Logger log = Logger.get(BackupMetadataManager.class);

    private final IDBI dbi;
    private final DaoSupplier<BackupMetadataDao> backupMetadataDaoSupplier;
    private final BackupMetadataDao dao;

    @Inject
    public BackupMetadataManager(
            @ForMetadata IDBI dbi,
            DaoSupplier<BackupMetadataDao> backupMetadataDaoSupplier
            )
    {
        this.dbi = requireNonNull(dbi, "dbi is null");
        this.backupMetadataDaoSupplier = requireNonNull(backupMetadataDaoSupplier, "shardDaoSupplier is null");
        this.dao = backupMetadataDaoSupplier.onDemand();

        createCenteraBackupMetadataTablesWithRetry(dbi);
    }
}
