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

import com.facebook.presto.spi.PrestoException;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.Properties;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_BACKUP_ERROR;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_BACKUP_NOT_FOUND;

import static java.util.Objects.requireNonNull;

public class CenteraBackupConfig
{
    private Properties prop;
    private String poolAddress;
    private long retentionPeriod;
    private String metadataFile;

    @NotNull
    public CenteraBackupConfig getConfig()
    {
        return this;
    }

    @Config("centera.config")
    @ConfigDescription("Config detailing object store connection")
    public CenteraBackupConfig setConfig(String configFilePath)
    {
        requireNonNull(configFilePath, "centera config is null");
        try {
            FileInputStream configIs = new FileInputStream(new File(configFilePath));
            prop = new Properties();
            prop.load(configIs);
        }
        catch (FileNotFoundException e) {
            throw new PrestoException(RAPTOR_BACKUP_NOT_FOUND, "File " + configFilePath + " not found. " + e);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Failed to read " + configFilePath + e);
        }

        poolAddress = prop.getProperty("centera.pooladdress");
        retentionPeriod = Long.parseLong(prop.getProperty("centera.retentionperiod"));
        metadataFile = prop.getProperty("centera.metadatafile");
        return this;
    }

    public String getPoolAddress()
    {
        return poolAddress;
    }

    public long getRetentionPeriod()
    {
        return retentionPeriod;
    }

    public String getMetadataFile()
    {
        return metadataFile;
    }
}
