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

import com.google.common.base.Throwables;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public final class BackupSchemaDaoUtil
{
    private static final Logger log = Logger.get(BackupSchemaDaoUtil.class);

    private BackupSchemaDaoUtil() {}

    public static void createCenteraBackupMetadataTablesWithRetry(IDBI dbi)
    {
        Duration delay = new Duration(2, TimeUnit.SECONDS);
        while (true) {
            try (Handle handle = dbi.open()) {
                if (handle.getConnection().getMetaData().getDatabaseProductName().equalsIgnoreCase("postgresql")) {
                    createBackupMetadataTables(handle.attach(PGBackupSchemaDao.class));
                }
                else {
                    createBackupMetadataTables(handle.attach(BackupSchemaDao.class));
                }
                return;
            }
            catch (UnableToObtainConnectionException e) {
                log.warn("Failed to connect to database. Will retry again in %s. Exception: %s", delay, e.getMessage());
                sleep(delay);
            }
            catch (SQLException e) {
                log.warn("Failed to get database connection metadata. Will retry again in %s. Exception: %s", delay, e.getMessage());
                sleep(delay);
            }
        }
    }

    private static void createBackupMetadataTables(BackupSchemaDao dao)
    {
        dao.createCenteraMetadataTable();
    }

    private static void sleep(Duration duration)
    {
        try {
            Thread.sleep(duration.toMillis());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
    }
}
