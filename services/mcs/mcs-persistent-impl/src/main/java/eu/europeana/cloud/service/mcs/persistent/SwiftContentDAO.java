package eu.europeana.cloud.service.mcs.persistent;

import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingInputStream;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.io.Payload;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

/**
 * Provides DAO operations for Openstack Swift.
 */
@Repository
public class SwiftContentDAO implements ContentDAO {

    @Autowired
    private SwiftConnectionProvider connectionProvider;


    /**
     * Puts given content to storage under given fileName. Counts and returns content length and md5 checksum from given
     * data.
     * 
     * @param fileName
     *            name of the file
     * @param data
     *            content of file to be saved
     * @return md5 and content length
     * @throws IOException
     *             if an I/O error occurs
     */
    @Override
    public PutResult putContent(String fileName, InputStream data)
            throws IOException {

        BlobStore blobStore = connectionProvider.getBlobStore();
        String container = connectionProvider.getContainer();
        CountingInputStream countingInputStream = new CountingInputStream(data);
        DigestInputStream md5DigestInputStream = md5InputStream(countingInputStream);
        Blob blob = blobStore.blobBuilder(fileName).name(fileName).payload(md5DigestInputStream).build();
        blobStore.putBlob(container, blob);
        String md5 = BaseEncoding.base16().lowerCase().encode(md5DigestInputStream.getMessageDigest().digest());
        Long contentLength = countingInputStream.getCount();
        return new PutResult(md5, contentLength);
    }


    /**
     * Retrieves content of file from storage. Can retrieve range of bytes of the file.
     * 
     * @param fileName
     *            name of the file to retrieve
     * @param start
     *            first offset included in the response. If equal to -1, ignored.
     * @param end
     *            last offset included in the response (inclusive). If equal to -1, ignored.
     * @param os
     *            outputstream the content is written to
     * @throws IOException
     *             if an I/O error occurs
     * @throws FileNotExistsException
     *             if object does not exist in the storage
     */
    @Override
    public void getContent(String fileName, long start, long end, OutputStream os)
            throws IOException, FileNotExistsException {
        BlobStore blobStore = connectionProvider.getBlobStore();
        String container = connectionProvider.getContainer();

        if (!blobStore.blobExists(connectionProvider.getContainer(), fileName)) {
            throw new FileNotExistsException(String.format("File %s not exists", fileName));
        }

        GetOptions options = GetOptions.NONE;
        if (start > -1 && end > -1) {
            options = new GetOptions().range(start, end);
        } else if (start > -1 && end == -1) {
            options = new GetOptions().startAt(start);
        } else if (start == -1 && end > -1) {
            options = new GetOptions().range(0, end);
        }
        Payload payload = blobStore.getBlob(container, fileName, options).getPayload();
        if (payload != null) {
            ByteStreams.copy(payload.getInput(), os);
        }
    }


    /**
     * Copies content of one storage object to another.
     * 
     * @param sourceObjectId
     *            name of the source storage object
     * @param trgObjectId
     *            name of the target storage object
     * @throws FileNotExistsException
     *             if source object does not exist in the storage
     */
    @Override
    public void copyContent(String sourceObjectId, String trgObjectId)
            throws FileNotExistsException, FileAlreadyExistsException {
        BlobStore blobStore = connectionProvider.getBlobStore();
        String container = connectionProvider.getContainer();
        if (!blobStore.blobExists(connectionProvider.getContainer(), sourceObjectId)) {
            throw new FileNotExistsException(String.format("File %s not exists", sourceObjectId));
        }
        if (blobStore.blobExists(connectionProvider.getContainer(), trgObjectId)) {
            throw new FileAlreadyExistsException(String.format("Target file %s already exists", trgObjectId));
        }
        Blob blob = blobStore.getBlob(container, sourceObjectId);
        Blob newBlob = blobStore.blobBuilder(trgObjectId).name(trgObjectId).payload(blob.getPayload()).build();
        blobStore.putBlob(container, newBlob);
    }


    /**
     * Deletes storage object identified by fileName.
     * 
     * @param fileName
     *            name of the object to be deleted
     * @throws FileNotExistsException
     *             if object does not exist in the storage
     */
    @Override
    public void deleteContent(String fileName)
            throws FileNotExistsException {
        BlobStore blobStore = connectionProvider.getBlobStore();
        String container = connectionProvider.getContainer();
        if (!blobStore.blobExists(connectionProvider.getContainer(), fileName)) {
            throw new FileNotExistsException(String.format("File %s not exists", fileName));
        }
        blobStore.removeBlob(container, fileName);
    }


    private DigestInputStream md5InputStream(InputStream is) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return new DigestInputStream(is, md);
        } catch (NoSuchAlgorithmException ex) {
            throw new AssertionError("Cannot get instance of MD5 but such algorithm should be provided", ex);
        }
    }
}
