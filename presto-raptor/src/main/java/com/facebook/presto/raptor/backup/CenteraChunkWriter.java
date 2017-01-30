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

import com.filepool.fplibrary.FPLibraryException;
import com.filepool.fplibrary.FPTag;

import io.airlift.log.Logger;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_BACKUP_ERROR;

public class CenteraChunkWriter implements Runnable
{
    private FPTag   mTag;
    private long    mOptions;
    private long    mSequenceId;
    private CenteraPartialFileInputStream mStream;
    private Exception mException;
    private int     mStatus;

    private Logger logger = Logger.get(this.getClass());

    CenteraChunkWriter(FPTag tag, CenteraPartialFileInputStream inStream, long options, long sequenceId)
    {
        mTag = tag;
        mOptions = options;
        mSequenceId = sequenceId;
        mStream = inStream;

        mException = null;
        mStatus = 0;
    }

    public Exception getmException()
    {
        return mException;
    }

    public void setmException(Exception mException)
    {
        this.mException = mException;
    }

    public int getmStatus()
    {
        return mStatus;
    }

    public void setmStatus(int mStatus)
    {
        this.mStatus = mStatus;
    }

    public void run()
    {
        try {
            logger.debug("Transferring chunk number " + mSequenceId);
            mTag.BlobWritePartial(mStream, mOptions, mSequenceId);
            logger.debug("Completed transfer of chunk number" + mSequenceId);
        }
        catch (FPLibraryException e) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Centera write exception occurred when writing chunk " + mSequenceId + e);
            /*mException = e;
            mStatus = e.getErrorCode();*/
        }
        catch (Exception e) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Unknown exception occurred when writing chunk " + mSequenceId + e);
            /*mException = e;
            mStatus = -1;*/
        }
    }
}
