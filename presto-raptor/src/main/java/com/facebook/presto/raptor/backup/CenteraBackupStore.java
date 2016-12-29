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

import com.filepool.fplibrary.FPClip;
import com.filepool.fplibrary.FPFileInputStream;
import com.filepool.fplibrary.FPLibraryConstants;
import com.filepool.fplibrary.FPLibraryException;
import com.filepool.fplibrary.FPPool;
import com.filepool.fplibrary.FPTag;

import io.airlift.log.Logger;

import javax.inject.Inject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_BACKUP_ERROR;
import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_BACKUP_NOT_FOUND;

public class CenteraBackupStore implements BackupStore
{
    private final CenteraBackupConfig config;
    private final String appVersion = "0.1";
    private final String appName = "PrestoRaptorBackup";
    private final String vendorName = "InnomindsCS";
    private final String tagName = "RaptorBackup";
    private final String clipName = "RaptorShard";

    private Logger logger = Logger.get(this.getClass());

    private FPPool thePool = null;

    @Inject
    public CenteraBackupStore(CenteraBackupConfig config)
    {
        this.config = config;
        try {
            FPPool.RegisterApplication(appName, appVersion);

            // New feature for 2.3 lazy pool open
            FPPool.setGlobalOption(FPLibraryConstants.FP_OPTION_OPENSTRATEGY, FPLibraryConstants.FP_LAZY_OPEN);

            thePool = new FPPool(this.config.getPoolAddress());
        }
        catch (FPLibraryException e) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Failed to register centra backup application ", e);
        }
    }

    @Override
    public void backupShard(UUID uuid, File source)
    {
        String clipId = null;
        // Optimistically assume the file can be created
        logger.info("Attempting to Write shard %s to centera.", source.getPath());
        try {
            clipId = storeFile(source);
            // Write UUID=clipId in the metadata file
            logger.info("Writing clipId %s for shard %s to metadata.", clipId, uuid.toString());
            saveToMetadata(uuid, clipId);
        }
        catch (FPLibraryException e) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Failed to backup shard: " + uuid, e);
        }
    }

    @Override
    public void restoreShard(UUID uuid, File target)
    {
        try {
            logger.info("Attempting to retrieve shard with UUID from Centera: " + uuid.toString() + " ... ");
            retrieveShard(uuid, target);
        }
        catch (FileNotFoundException e) {
            throw new PrestoException(RAPTOR_BACKUP_NOT_FOUND, "Backup shard not found: " + uuid, e);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Failed to copy backup shard: " + uuid, e);
        }
        logger.info("Successfully retrieved shard with UUID from Centera: " + uuid.toString());
    }

    @Override
    public boolean deleteShard(UUID uuid)
    {
        String clipId = getClipidfromMetadata(uuid);
        boolean deleted = false;

        if (clipId == null || clipId.equals("")) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Got null or invalid clipId from Centera for shard " + uuid);
        }

        try {
            if (!FPClip.Exists(thePool, clipId)) {
                throw new IllegalArgumentException("ClipID \"" + clipId + "\" does not exist on this Centera cluster.");
            }
            logger.info("Attempting to delete shard with UUID " + uuid.toString() + "whose clipId is " + clipId + "from Centera: ");
            FPClip.Delete(thePool, clipId);
            logger.info("Successfully deleted shard with UUID " + uuid.toString() + "whose clipId is " + clipId + "from Centera: ");
            deleted = true;
        }
        catch (FPLibraryException e) {
            throw new PrestoException(RAPTOR_BACKUP_ERROR, "Failed to delete shard: " + uuid, e);
        }
        return deleted;
    }

    @Override
    public boolean shardExists(UUID uuid)
    {
        Properties metaProp = getMetadataAsProperties();

        if (metaProp.getProperty(uuid.toString()) != null) {
            return true;
        }
        return false;
    }

    private String storeFile(File source) throws FPLibraryException
    {
        String clipID = "";

        try {
            // create a new named C-Clip
            FPClip theClip = new FPClip(thePool, clipName);

            // It's a good practice to write out vendor, application and version
            // info
            theClip.setDescriptionAttribute("app-vendor", vendorName);
            theClip.setDescriptionAttribute("app-name", appName);
            theClip.setDescriptionAttribute("app-version", appVersion);

            // It's a good idea to explicitly set retention period. For more
            // info
            // on retention periods and classes see ManageRetention example.
            logger.info("Retention period is " + config.getRetentionPeriod());
            theClip.setRetentionPeriod(config.getRetentionPeriod());

            FPFileInputStream inputStream = new FPFileInputStream(source);

            FPTag topTag = theClip.getTopTag();

            FPTag newTag = new FPTag(topTag, tagName);

            topTag.Close();

            // Blob size is written to clip, so lets just write out filename.
            newTag.setAttribute("filename", source.getName());

            // write the binary data for this tag to the Centera
            newTag.BlobWrite(inputStream);

            clipID = theClip.Write();

            inputStream.close();
            newTag.Close();
            theClip.Close();
        }
        catch (FileNotFoundException e) {
            throw new IllegalArgumentException("Could not open file \"" + source.getName() + "\" for reading");
        }
        catch (IOException e) {
        }

        return (clipID);
    }

    public void retrieveShard(UUID uuid, File saveFilename) throws FileNotFoundException, IOException
    {
        String clipId = "";
        try {
            Properties metadata = getMetadataAsProperties();

            if (metadata != null) {
                clipId = metadata.getProperty(uuid.toString());
            }

            if (clipId == null) {
                throw new PrestoException(RAPTOR_BACKUP_NOT_FOUND, "Backup shard not found: " + uuid);
            }

            // Contact cluster to load C-Clip
            logger.info("Attempting to retrieve C-Clip with clip ID: " + clipId + " ... ");

            FPClip theClip = new FPClip(thePool, clipId, FPLibraryConstants.FP_OPEN_FLAT);

            logger.info("Retrieve of clip " + clipId + " Successful");

            FPTag topTag = theClip.getTopTag();

            // check clip metadata to see if this is 'our' data format
            if (!topTag.getTagName().equals(tagName)) {
                logger.error("This clip was not written by Raptor.");
                logger.error(topTag.getTagName());
                logger.error(tagName);
            }

            // Save blob data to file 'OrigFilename.out'
            FileOutputStream outFile = new FileOutputStream(saveFilename);
            topTag.BlobRead(outFile);

            outFile.close();
            topTag.Close();
            theClip.Close();
        }
        catch (FPLibraryException e) {
            logger.error("Centera SDK Error: " + e.getMessage());
        }
        catch (IOException e) {
            logger.error("IO Error occured: " + e.getMessage());
        }
    }

    public void saveToMetadata(UUID uuid, String clipId)
    {
        try {
            FileWriter clipIDWriter = new FileWriter(config.getMetadataFile(), true);
            clipIDWriter.write(uuid.toString() + "=" + clipId);
            clipIDWriter.write("\n");
            clipIDWriter.close();
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Problem saving clip ID to file \"" + config.getMetadataFile()
            + "\"\nObject successfully stored as clip ID \"" + clipId);
        }
    }

    private Properties getMetadataAsProperties()
    {
        Properties metadata = new Properties();
        try {
            FileInputStream metaFileIs = new FileInputStream(new File(config.getMetadataFile()));
            metadata.load(metaFileIs);
            return metadata;
        }
        catch (IOException e) {
            logger.error("IO Error occured while reading metadata" + e.getMessage());
        }
        return null;
    }

    private String getClipidfromMetadata(UUID uuid)
    {
        Properties metadata = getMetadataAsProperties();
        String clipId = null;

        if (metadata != null) {
            clipId = metadata.getProperty(uuid.toString());
        }
        return clipId;
    }
}
