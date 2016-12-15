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

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
//import com.google.common.io.ByteSource;
//import com.google.common.io.Resources;

import java.io.File;
import java.io.FileInputStream;
//import java.lang.Integer;
//import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

public class ArchiveRecordSet
        implements RecordSet
{
    private final List<ArchiveColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final String fileName;
    private final ArchiveTransformInfo archiveTransformInfo;

    public ArchiveRecordSet(ArchiveSplit split, List<ArchiveColumnHandle> columnHandles)
    {
        requireNonNull(split, "split is null");
        requireNonNull(split.getFileName(), "ArchiveRecordSet: fileName is null");

        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ArchiveColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
        this.fileName = split.getFileName();

        this.archiveTransformInfo = getArchiveTransformInfo(split);
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new ArchiveRecordCursor(columnHandles, fileName, archiveTransformInfo);
    }

    public ArchiveTransformInfo getArchiveTransformInfo(ArchiveSplit split)
    {
        if (split.getTransformInfoFile() != null) {
           Properties transformProp = new Properties();

           try {
               FileInputStream is = new FileInputStream(new File(split.getTransformInfoFile()));
               transformProp.load(is);
           }
           catch (Exception e) {
                 throw Throwables.propagate(e);
           }

           String schemTableDotted = split.getSchemaName() + "." + split.getTableName() + ".";

           String transform = transformProp.getProperty(schemTableDotted + "transform");

           if (transform != null && transform.equalsIgnoreCase("true")) {
                   int numStages = Integer.parseInt(transformProp.getProperty(schemTableDotted + "numStages"));
                   Map<String, String> stageProcessor = new HashMap<String, String>();
                   Map<String, String> stageInput = new HashMap<String, String>();
                   Map<String, String> stageOutput = new HashMap<String, String>();

                   // Always start processing from index 1, since we want it named like stage1, stage2...
                   for (int i = 1; i <= numStages; i++) {
                        String stage = schemTableDotted + "stage" + i;
                        String processor = transformProp.getProperty(stage);
                        String stageIn = transformProp.getProperty(stage + "." + "input");
                        String stageOut = transformProp.getProperty(stage + "." + "output");

                        if (processor != null) {
                              stageProcessor.put("stage" + i, transformProp.getProperty(schemTableDotted + "stage" + i));
                        }
                        if (stageIn != null) {
                            stageInput.put("stage" + i, stageIn);
                        }
                        if (stageOut != null) {
                            stageOutput.put("stage" + i, stageIn);
                        }
                   }
                   return new ArchiveTransformInfo(numStages, stageProcessor, stageInput, stageOutput);
           }
        }
        return null;
    }
}
