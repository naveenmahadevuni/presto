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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDuration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ShardCleanerConfig
{
    private Duration maxTransactionAge = new Duration(1, DAYS);
    private Duration transactionCleanerInterval = new Duration(10, MINUTES);
    private Duration localCleanerInterval = new Duration(1, HOURS);
    private Duration localCleanTime = new Duration(4, HOURS);
    private Duration oldLocalShardCleanerInterval = new Duration(4, HOURS);
    private Duration oldLocalShardCleanTime = new Duration(5, MINUTES);
    private Duration localShardSpaceCheckInterval = new Duration(6, HOURS);
    private DataSize minDiskSpaceLoadQuery = new DataSize(2560, MEGABYTE);
    private Duration backupCleanerInterval = new Duration(5, MINUTES);
    private Duration backupCleanTime = new Duration(1, DAYS);
    private int backupDeletionThreads = 50;
    private Duration maxCompletedTransactionAge = new Duration(1, DAYS);

    @NotNull
    @MinDuration("1m")
    @MaxDuration("30d")
    public Duration getMaxTransactionAge()
    {
        return maxTransactionAge;
    }

    @Config("raptor.max-transaction-age")
    @ConfigDescription("Maximum time a transaction may run before it is aborted")
    public ShardCleanerConfig setMaxTransactionAge(Duration maxTransactionAge)
    {
        this.maxTransactionAge = maxTransactionAge;
        return this;
    }

    @NotNull
    @MinDuration("1m")
    public Duration getTransactionCleanerInterval()
    {
        return transactionCleanerInterval;
    }

    @Config("raptor.transaction-cleaner-interval")
    @ConfigDescription("How often to cleanup expired transactions")
    public ShardCleanerConfig setTransactionCleanerInterval(Duration transactionCleanerInterval)
    {
        this.transactionCleanerInterval = transactionCleanerInterval;
        return this;
    }

    @NotNull
    @MinDuration("1m")
    public Duration getLocalCleanerInterval()
    {
        return localCleanerInterval;
    }

    @Config("raptor.local-cleaner-interval")
    @ConfigDescription("How often to discover local shards that need to be cleaned up")
    public ShardCleanerConfig setLocalCleanerInterval(Duration localCleanerInterval)
    {
        this.localCleanerInterval = localCleanerInterval;
        return this;
    }

    @NotNull
    public Duration getLocalCleanTime()
    {
        return localCleanTime;
    }

    @Config("raptor.local-clean-time")
    @ConfigDescription("How long to wait after discovery before cleaning local shards")
    public ShardCleanerConfig setLocalCleanTime(Duration localCleanTime)
    {
        this.localCleanTime = localCleanTime;
        return this;
    }

    @NotNull
    @MinDuration("1m")
    public Duration getOldLocalShardCleanerInterval()
    {
        return oldLocalShardCleanerInterval;
    }

    @Config("raptor.old-local-shard-cleaner-interval")
    @ConfigDescription("How often to discover old local shards that need to be cleaned up")
    public ShardCleanerConfig setOldLocalShardCleanerInterval(Duration oldLocalShardCleanerInterval)
    {
        this.oldLocalShardCleanerInterval = oldLocalShardCleanerInterval;
        return this;
    }

    @NotNull
    public Duration getOldLocalShardCleanTime()
    {
        return oldLocalShardCleanTime;
    }

    @Config("raptor.old-local-shard-clean-time")
    @ConfigDescription("How long to wait after retrieval from backup before cleaning local shards")
    public ShardCleanerConfig setOldLocalShardCleanTime(Duration oldLocalShardCleanTime)
    {
        this.oldLocalShardCleanTime = oldLocalShardCleanTime;
        return this;
    }

    @NotNull
    @MinDuration("1m")
    public Duration getLocalShardSpaceCheckInterval()
    {
        return localShardSpaceCheckInterval;
    }

    @Config("raptor.local-shard-space-check-interval")
    @ConfigDescription("How often to check local shard space usage")
    public ShardCleanerConfig setLocalShardSpaceCheckInterval(Duration localShardSpaceCheckInterval)
    {
        this.localShardSpaceCheckInterval = localShardSpaceCheckInterval;
        return this;
    }

    @NotNull
    public DataSize getMinDiskSpaceLoadQuery()
    {
        return minDiskSpaceLoadQuery;
    }

    @Config("raptor.min-disk-space-load-query")
    @ConfigDescription("How much disk space in Mega Bytes reserved for Raptor load, query operation and minimum free space")
    public ShardCleanerConfig setMinDiskSpaceLoadQuery(DataSize minDiskSpaceLoadQuery)
    {
        this.minDiskSpaceLoadQuery = minDiskSpaceLoadQuery;
        return this;
    }

    @NotNull
    @MinDuration("1m")
    public Duration getBackupCleanerInterval()
    {
        return backupCleanerInterval;
    }

    @Config("raptor.backup-cleaner-interval")
    @ConfigDescription("How often to check for backup shards that need to be cleaned up")
    public ShardCleanerConfig setBackupCleanerInterval(Duration backupCleanerInterval)
    {
        this.backupCleanerInterval = backupCleanerInterval;
        return this;
    }

    @NotNull
    public Duration getBackupCleanTime()
    {
        return backupCleanTime;
    }

    @Config("raptor.backup-clean-time")
    @ConfigDescription("How long to wait after deletion before cleaning backup shards")
    public ShardCleanerConfig setBackupCleanTime(Duration backupCleanTime)
    {
        this.backupCleanTime = backupCleanTime;
        return this;
    }

    @Min(1)
    public int getBackupDeletionThreads()
    {
        return backupDeletionThreads;
    }

    @Config("raptor.backup-deletion-threads")
    @ConfigDescription("Maximum number of threads to use for deleting shards from backup store")
    public ShardCleanerConfig setBackupDeletionThreads(int backupDeletionThreads)
    {
        this.backupDeletionThreads = backupDeletionThreads;
        return this;
    }

    @NotNull
    @MinDuration("1m")
    @MaxDuration("30d")
    public Duration getMaxCompletedTransactionAge()
    {
        return maxCompletedTransactionAge;
    }

    @Config("raptor.max-completed-transaction-age")
    @ConfigDescription("Maximum time a record of a successful or failed transaction is kept")
    public ShardCleanerConfig setMaxCompletedTransactionAge(Duration maxCompletedTransactionAge)
    {
        this.maxCompletedTransactionAge = maxCompletedTransactionAge;
        return this;
    }
}
