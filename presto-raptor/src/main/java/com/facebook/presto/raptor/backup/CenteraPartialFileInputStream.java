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

import com.filepool.fplibrary.FPStreamInterface;

import io.airlift.log.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

public class CenteraPartialFileInputStream extends InputStream implements FPStreamInterface
{
    private RandomAccessFile mFile;
    private long             mMarkPos = 0;
    private long             mPartialLength = -1;
    private long             mPartialEOF    = -1;

    private Logger logger = Logger.get(this.getClass());

    public CenteraPartialFileInputStream(File pFile, long pOffset, long pLen) throws FileNotFoundException, IOException
    {
        mFile = new RandomAccessFile(pFile.getAbsolutePath(), "r");

        mPartialLength  = pLen;

        mFile.seek(pOffset); // set the file point to the offset pos
        mMarkPos = pOffset;  // default to the beginning part of this stream.

        mPartialEOF     = pOffset + mPartialLength;
    }

    public boolean FPMarkSupported()
    {
        return true;
    }

    public void FPMark()
    {
        try {
            mMarkPos = mFile.getFilePointer();
        }
        catch (IOException e) {
            logger.error("IOException while setting file pointer");
        }
    }

    public void FPReset() throws IOException
    {
        mFile.seek(mMarkPos);
    }

    /**
     * Always return true: marks are supported on random access files.
     */
    public boolean markSupported()
    {
        return FPMarkSupported();
    }

    /**
     * Remembers the current position in the file.
     * @param readLimit is not used
     */
    public void mark(int readlimit)
    {
        FPMark();
    }

    /**
     * Go back to our last marked position (or to the beginning of the file)
     */
    public void reset() throws IOException
    {
        FPReset();
    }

    /**
     * @return false if this stream is not a partial stream
     *         true if all of the following is true:
     *              - This stream is a partial stream
     *              - The file pointer is pointing to a part of the underlying file
     *                that is past the artificial EOF of this partial stream
     */
    private boolean isPartStreamAtEOF()
    {
        boolean vRetVal = false;

        try {
            long vCurrOffsetIntoFile = mFile.getFilePointer();
            if (vCurrOffsetIntoFile >= mPartialEOF) {
                vRetVal = true;
            }
        }
        catch (IOException ioe) {
            vRetVal = true;
        }
        return vRetVal;
    }

    /**
     * Reads a byte of data from this input stream.
     * @return     the next byte of data, or <code>-1</code> if the end of the
     *             file is reached.
     * @exception  IOException  if an I/O error occurs.
     */
    public int read() throws IOException
    {
        int vRetVal = -1;
        if (isPartStreamAtEOF() == false) {
            vRetVal = mFile.read();
        }

        return vRetVal;
    }

    /**
     * Reads an array of bytes from the input stream
     * @param b    the buffer into which the data is read.
     * @return     the total number of bytes read into the buffer, or
     *             <code>-1</code> if there is no more data because the end of
     *             the file has been reached.
     * @exception  IOException  if an I/O error occurs.
     */

    public int read(byte[] b) throws IOException
    {
        return mFile.read(b, 0, b.length);
    }

    /**
     * Reads an array of bytes from the input stream
     * @param b    the buffer into which the data is read.
     * @param      off   the start offset of the data.
     * @param      len   the maximum number of bytes read.
     * @return     the total number of bytes read into the buffer, or
     *             <code>-1</code> if there is no more data because the end of
     *             the file has been reached.
     * @exception  IOException  if an I/O error occurs.
     */
    public int read(byte[] b, int off, int len) throws IOException
    {
        int vRetVal = -1;
        long vPosition;

        try {
            vPosition = mFile.getFilePointer();
        }
        catch (IOException ioe) {
            return vRetVal;
        }

        // If the amount remaining to be read in the segment is less than the SDK requested
        // then adjust it to the amount remaining
        if ((vPosition + len) >= mPartialEOF) {
            len = (int) (mPartialEOF - vPosition);
        }

        if (isPartStreamAtEOF() == false) {
            vRetVal = mFile.read(b, 0, len);
        }
        return vRetVal;
    }

    /**
     * Close the file
     */
    public void close() throws IOException
    {
        if (mFile != null) {
            mFile.close();
        }
        mFile = null;
    }

    /**
     * Be sure the file is closed
     */
    protected void finalize()
    {
        if (mFile != null) {
            try {
                close();
            }
            catch (Exception e) {
                logger.error("Exception occurred while the file is being closed");
            }
        }
    }

    /**
     * Implements the FPInputStream interface method
     */
    public long getStreamLength()
    {
        return (mPartialLength);
    }
}
