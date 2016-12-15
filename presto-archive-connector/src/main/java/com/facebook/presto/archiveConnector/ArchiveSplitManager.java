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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

//import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.io.File;
import java.io.FileInputStream;

import java.util.ArrayList;
import java.util.Arrays;
//import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
//import java.util.stream.Collectors;
import static com.facebook.presto.archiveConnector.Types.checkType;
//import com.google.common.base.Throwables;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class ArchiveSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final ArchiveClient archiveClient;
    private final NodeManager nodeManager;

    @Inject
    public ArchiveSplitManager(ArchiveConnectorId connectorId, ArchiveClient archiveClient, NodeManager nodeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.archiveClient = requireNonNull(archiveClient, "client is null");
        this.nodeManager = nodeManager;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        ArchiveTableLayoutHandle layoutHandle = checkType(layout, ArchiveTableLayoutHandle.class, "layout");
        ArchiveTableHandle tableHandle = layoutHandle.getTable();
        ArchiveTable table = archiveClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists", tableHandle.getSchemaName(), tableHandle.getTableName());

        // Read the input files info

        Properties inputFileProp = new Properties();

        try {
            FileInputStream is = new FileInputStream(archiveClient.getConfig().getInputFilesInfo());
            inputFileProp.load(is);
        }
        catch (Exception e) {
        }

        String tableSchemaString = tableHandle.getSchemaName() + "." + tableHandle.getTableName() + ".";

        String readFromInputDir = inputFileProp.getProperty(tableSchemaString + "read-files-from-input-dir");
        String inputDir = null;

        if (readFromInputDir.equalsIgnoreCase("true")) {
          inputDir = inputFileProp.getProperty(tableSchemaString + "inputDirPath");
        }

        // checkState(archiveClient.getInputFilesInfo() == null, "inputFileInfo is Null", archiveClient.getInputFilesInfo());
        //checkState(inputDir == null, "inputDir is Null", tableSchemaString + "read-files-from-input-dir:", archiveClient.getInputFilesInfo());

        File inputPath = new File(inputDir);

        checkState(inputPath != null, "ArchiveSplitManager: inputDir is Null", inputDir, inputPath);

        File[] listOfFiles = inputPath.listFiles();

        List<ConnectorSplit> splits = new ArrayList<>();

        int numOfNodes = nodeManager.getAllNodes().size();
        int numOfFiles = listOfFiles.length;

        int filesPerNode = numOfFiles / numOfNodes;

        if (numOfNodes > numOfFiles) {
                numOfFiles = 1;
        }

        Iterator<File> files = Arrays.asList(listOfFiles).iterator();

        for (Node node : nodeManager.getAllNodes()) {
                Boolean hasOneFile = false;

                for (int i = 0; i < filesPerNode; i++) {
                   if (files.hasNext()) {
                       hasOneFile = true;
                       splits.add(new ArchiveSplit(connectorId, tableHandle.getSchemaName(), tableHandle.getTableName(), node.getHostAndPort(), files.next().getPath(), archiveClient.getConfig().getTransformInfo()));
                   }
                }
        }
        return new FixedSplitSource(splits);
    }
}
