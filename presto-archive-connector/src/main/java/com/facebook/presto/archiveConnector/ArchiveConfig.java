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
package com.facebook.presto.archiveConnector;

import io.airlift.configuration.Config;

import javax.validation.constraints.NotNull;

import java.net.URI;

public class ArchiveConfig
{
    private String archiveServerName;
    private String archiveServerPool;
    private String archiveServerKey;
    private String archiveServerPeaFile;

    private Boolean readFromInputDir = false;
    private String inputDirPath;

    private URI metadata;

    private String inputFilesInfo;
    private String transformInfo;

    @NotNull
    public String getArchiveServerName()
    {
        return archiveServerName;
    }

    @Config("archive-server-name")
    public ArchiveConfig setArchiveServerName(String archiveServerName)
    {
        this.archiveServerName = archiveServerName;
        return this;
    }

     @NotNull
    public String getArchiveServerPool()
    {
        return archiveServerPool;
    }

    @Config("archive-server-pool")
    public ArchiveConfig setArchiveServerPool(String archiveServerPool)
    {
        this.archiveServerPool = archiveServerPool;
        return this;
    }

    @NotNull
    public String getArchiveServerKey()
    {
        return archiveServerKey;
    }

    @Config("archive-server-key")
    public ArchiveConfig setArchiveServerKey(String archiveServerKey)
    {
        this.archiveServerKey = archiveServerKey;
        return this;
    }

    @NotNull
    public String getArchiveServerPeaFile()
    {
        return archiveServerPeaFile;
    }

    @Config("archive-server-peafile")
    public ArchiveConfig setArchiveServerPeaFile(String archiveServerPeaFile)
    {
        this.archiveServerPeaFile = archiveServerPeaFile;
        return this;
    }

    @NotNull
    public URI getMetadata()
    {
        return metadata;
    }

    @Config("metadata-uri")
    public ArchiveConfig setMetadata(URI metadata)
    {
        this.metadata = metadata;
        return this;
    }

    @NotNull
    public String getInputFilesInfo()
    {
        return inputFilesInfo;
    }

    @Config("input-files-info")
    public ArchiveConfig setInputFilesInfo(String inputFilesInfo)
    {
        this.inputFilesInfo = inputFilesInfo;
        return this;
    }

    public String getTransformInfo()
    {
        return transformInfo;
    }

    @Config("transform-info")
    public ArchiveConfig setTransformInfo(String transformInfo)
    {
        this.transformInfo = transformInfo;
        return this;
    }
}
