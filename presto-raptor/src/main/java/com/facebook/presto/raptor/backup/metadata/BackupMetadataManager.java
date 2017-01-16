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
//import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.raptor.util.DaoSupplier;
import io.airlift.log.Logger;
//import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
//import org.skife.jdbi.v2.ResultIterator;
//import org.skife.jdbi.v2.exceptions.DBIException;
//import org.skife.jdbi.v2.tweak.HandleConsumer;
//import org.skife.jdbi.v2.util.ByteArrayMapper;

import javax.inject.Inject;
/*
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
*/
//import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
//import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_EXTERNAL_BATCH_ALREADY_EXISTS;
import static com.facebook.presto.raptor.backup.metadata.BackupSchemaDaoUtil.createCenteraBackupMetadataTablesWithRetry;
/*
import static com.facebook.presto.raptor.storage.ColumnIndexStatsUtils.jdbcType;
import static com.facebook.presto.raptor.storage.ShardStats.MAX_BINARY_INDEX_SIZE;
import static com.facebook.presto.raptor.util.ArrayUtil.intArrayFromBytes;
import static com.facebook.presto.raptor.util.ArrayUtil.intArrayToBytes;
import static com.facebook.presto.raptor.util.DatabaseUtil.bindOptionalInt;
import static com.facebook.presto.raptor.util.DatabaseUtil.isSyntaxOrAccessError;
import static com.facebook.presto.raptor.util.DatabaseUtil.metadataError;
import static com.facebook.presto.raptor.util.DatabaseUtil.runIgnoringConstraintViolation;
import static com.facebook.presto.raptor.util.DatabaseUtil.runTransaction;
import static com.facebook.presto.raptor.util.UuidUtil.uuidFromBytes;
import static com.facebook.presto.raptor.util.UuidUtil.uuidToBytes;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.facebook.presto.spi.StandardErrorCode.SERVER_STARTING_UP;
import static com.facebook.presto.spi.StandardErrorCode.TRANSACTION_CONFLICT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.propagateIfInstanceOf;
import static com.google.common.collect.Iterables.partition;
import static java.lang.Boolean.TRUE;
import static java.lang.Math.multiplyExact;
import static java.lang.String.format;
import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static java.util.Arrays.asList;
import static java.util.Collections.nCopies;
*/
import static java.util.Objects.requireNonNull;
/*
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
*/
public class BackupMetadataManager
{
    private static final Logger log = Logger.get(BackupMetadataManager.class);

    private final IDBI dbi;
    private final DaoSupplier<BackupMetadataDao> backupMetadataDaoSupplier;
    private final BackupMetadataDao dao;
//    private final NodeSupplier nodeSupplier;
//    private final AssignmentLimiter assignmentLimiter;
//    private final Duration startupGracePeriod;
//    private final long startTime;

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
