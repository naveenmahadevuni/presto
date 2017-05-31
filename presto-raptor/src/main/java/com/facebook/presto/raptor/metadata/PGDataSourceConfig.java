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
import io.airlift.configuration.DefunctConfig;
import io.airlift.dbpool.ManagedDataSourceConfig;

@DefunctConfig({"db.host", "db.port", "db.database", "db.ssl.enabled"})
public class PGDataSourceConfig extends ManagedDataSourceConfig<PGDataSourceConfig>
{
    private int defaultFetchSize = 100;

    /**
     * Gets the default fetch size for all connection.
     */
    public int getDefaultFetchSize()
    {
        return defaultFetchSize;
    }

    /**
     * Sets the default fetch size for all connection.
     */
    @Config("db.fetch-size")
    public PGDataSourceConfig setDefaultFetchSize(int defaultFetchSize)
    {
        this.defaultFetchSize = defaultFetchSize;
        return this;
    }
}
