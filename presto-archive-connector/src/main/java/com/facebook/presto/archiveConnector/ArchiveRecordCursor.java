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

//import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
//import com.google.common.io.ByteSource;
//import com.google.common.io.CountingInputStream;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;

import java.lang.reflect.Method;

import java.net.URL;
import java.net.URLClassLoader;

//import java.lang.ProcessBuilder;
//import java.lang.Process;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPInputStream;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.zip.GZIPInputStream.GZIP_MAGIC;
//import static java.nio.charset.StandardCharsets.UTF_8;

public class ArchiveRecordCursor
        implements RecordCursor
{
    private static final Splitter LINE_SPLITTER = Splitter.on(",").trimResults();

    private final List<ArchiveColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private final long totalBytes = 0;

    private List<File> filesList;
    private final FilesReader reader;

    private List<String> fields;

    public ArchiveRecordCursor(List<ArchiveColumnHandle> columnHandles, String fileName, ArchiveTransformInfo archiveTransformInfo)
    {
        this.columnHandles = columnHandles;

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            ArchiveColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }
       File file = new File(fileName);

       this.filesList = new ArrayList<>();
       this.filesList.add(file);
       requireNonNull(filesList, "filesList is null");

       this.reader = getFilesReader(this.filesList, columnHandles.size(), archiveTransformInfo);
    }

    private static FilesReader getFilesReader(List<File> filesList, int numOfColumns, ArchiveTransformInfo archiveTransformInfo)
    {
        try {
            return new FilesReader(filesList.iterator(), numOfColumns, archiveTransformInfo);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public long getTotalBytes()
    {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        try {
            fields = reader.readFields();
            if (fields != null) {
               checkArgument(fields.size() == columnHandles.size(), "Values retrieved doesn't match number of columns");
            }
            return fields != null;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");

        int columnIndex = fieldToColumnIndex[field];
        return fields.get(columnIndex);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        return Long.parseLong(getFieldValue(field));
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, createUnboundedVarcharType());
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    private int getNumOfColumns()
    {
        return columnHandles.size();
    }

    @Override
    public void close()
    {
    }

    private static class FilesReader
    {
        private final Iterator<File> files;
        private String currentFile;
        private int numOfColumns;
        private ArchiveTransformInfo archiveTransformInfo;
        private BufferedReader reader;

        public FilesReader(Iterator<File> files, int numOfColumns, ArchiveTransformInfo archiveTransformInfo)
                throws IOException
        {
            requireNonNull(files, "files is null");
            this.files = files;
            this.numOfColumns = numOfColumns;
            this.archiveTransformInfo = archiveTransformInfo;
            reader = createNextReader();
        }

        private BufferedReader createNextReader()
                throws IOException
        {
            if (!files.hasNext()) {
                return null;
            }

            BufferedReader br = null;

            File file = files.next();

            String outDir =  "/home/naveen/testing/presto-tests/outdir";

            if (archiveTransformInfo == null) {
                FileInputStream fileInputStream = new FileInputStream(file);
                currentFile = file.getPath();

                InputStream in = isGZipped(file) ? new GZIPInputStream(fileInputStream) : fileInputStream;
                br = new BufferedReader(new InputStreamReader(in));
            }
            else {
                requireNonNull(archiveTransformInfo, "archiveTransformInfo is null");
                int numStages = archiveTransformInfo.getNumStages();

                for (int i = 1; i <= numStages; i++) {
                    String stage = "stage" + i;
                    //checkState(archiveTransformInfo == null, "StageProcess: %s, filePath: %s, outFile : %s", archiveTransformInfo.getStageProcessor().get(stage), file.getPath(), outDir + "/" + file.getName());

                    String outFile1 = outDir + "/" + file.getName();
                    ProcessBuilder builder = new ProcessBuilder(archiveTransformInfo.getStageProcessor().get(stage), file.getPath(), outFile1);
                    Process process =  builder.start();

                    String className = "Adapter4ghw";
                    File adpJar = new File("/home/naveen/ifusion/adapters/Adapter4ghw.jar");
                    String adpOutPath = "/home/naveen/testing/presto-tests/adpout/";

                    URL[] urls = { adpJar.toURI().toURL() };
                    URLClassLoader loader = new URLClassLoader(urls);

                    try {
                      Class<?> cls = loader.loadClass(className);
                      //Class < cls.getClass() > adp = cls.newInstance();
                      Class[] cArgs = new Class[2];

                      cArgs[0] = String.class;
                      cArgs[1] = String.class;
                      Method transform = cls.getMethod("transformFile", cArgs);

                      String outFile2 = adpOutPath + file.getName();

                      transform.invoke(null, outFile1, outFile2);

                      file = new File(outFile2);
                      FileInputStream fileInputStream = new FileInputStream(file.getPath());
                         //currentFile = file.getPath();

                      InputStream in = isGZipped(file) ? new GZIPInputStream(fileInputStream) : fileInputStream;
                      br = new BufferedReader(new InputStreamReader(in));
                    }
                    catch (Exception e) {
                         Throwables.propagate(e);
                    }
                }
            }

            return br;
        }

        public static boolean isGZipped(File file)
                throws IOException
        {
            try (RandomAccessFile inputFile = new RandomAccessFile(file, "r")) {
                int magic = inputFile.read() & 0xff | ((inputFile.read() << 8) & 0xff00);
                return magic == GZIP_MAGIC;
            }
            catch (IOException e) {
                throw e;
            }
        }

        public List<String> readFields()
                throws IOException
        {
            List<String> fields = null;
            boolean newReader = false;

            while (fields == null) {
                if (reader == null) {
                    return null;
                }
                String line = reader.readLine();
                if (line != null) {
                    fields = LINE_SPLITTER.splitToList(line);
                    if (!newReader || meetsPredicate(fields)) {
                        /*if (fields != null) {
                            checkArgument(numOfColumns == fields.size(), "Retrieved values doesn't match number of columns for line %s in file %s", line, currentFile);
                        }*/
                        return fields;
                    }
                }
                reader.close();
                reader = createNextReader();
                newReader = true;
            }
            return fields;
        }

        private boolean meetsPredicate(List<String> fields)
        {
                return true;
        }

        public void close()
        {
            if (reader != null) {
                try {
                    reader.close();
                }
                catch (IOException ignored) {
                }
            }
        }
    }
}
