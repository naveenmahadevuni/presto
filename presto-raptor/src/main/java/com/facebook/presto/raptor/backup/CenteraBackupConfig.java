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
package com.facebook.presto.raptor.backup;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

public class CenteraBackupConfig
{
    private String poolAddress;
    private String configFilePath;

    @NotNull
    public CenteraBackupConfig getConfig()
    {
        return this;
    }

    @Config("backup.centera.pooladdress")
    @ConfigDescription("The pool address of the centera cluster")
    public CenteraBackupConfig setPoolAddress(String poolAddress)
    {
        this.poolAddress = poolAddress;
        return this;
    }

    @Config("backup.centera.config")
    @ConfigDescription("Config detailing object store connection")
    public CenteraBackupConfig setConfigFilePath(String configFilePath)
    {
        this.configFilePath =  configFilePath;
        return this;
    }

    public String getPoolAddress()
    {
        return poolAddress;
    }

    public String getConfigFilePath()
    {
        return configFilePath;
    }
}
