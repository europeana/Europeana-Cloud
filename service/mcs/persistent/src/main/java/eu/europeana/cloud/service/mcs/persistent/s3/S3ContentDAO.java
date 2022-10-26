package eu.europeana.cloud.service.mcs.persistent.s3;

import com.google.common.hash.HashCode;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.ContainerNotFoundException;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.io.Payload;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.*;

/**
 * Provides DAO operations for S3.
 */
@Repository
public class S3ContentDAO implements ContentDAO {

    private static final String MSG_FILE_NOT_EXISTS = "File %s not exists";
    private static final String MSG_TARGET_FILE_ALREADY_EXISTS = "Target file %s already exists";

    @Autowired
    private SwiftConnectionProvider connectionProvider;

    @Override
    public PutResult putContent(String fileName, InputStream data) throws IOException, ContainerNotFoundException {
        BlobStore blobStore = connectionProvider.getBlobStore();
        String container = connectionProvider.getContainer();
        BlobBuilder builder = blobStore.blobBuilder(fileName);
        builder = builder.name(fileName);
        ByteSource byteSource = ByteSource.wrap(IOUtils.toByteArray(data));
        String md5 = DigestUtils.md5Hex(IOUtils.toByteArray(byteSource.openStream()));
        builder = builder.payload(byteSource)
                .contentLength(byteSource.size())
                .contentMD5(HashCode.fromString(md5));
        Blob blob = builder.build();
        blobStore.putBlob(container, blob);
        return new PutResult(md5, byteSource.size());
    }

    @Override
    public void getContent(String fileName, long start, long end, OutputStream os)
            throws IOException, FileNotExistsException, ContainerNotFoundException {
        BlobStore blobStore = connectionProvider.getBlobStore();
        String container = connectionProvider.getContainer();

        if (!blobStore.blobExists(connectionProvider.getContainer(), fileName)) {
            throw new FileNotExistsException(String.format(MSG_FILE_NOT_EXISTS, fileName));
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
            ByteStreams.copy(payload.openStream(), os);
        }
    }

    @Override
    public void copyContent(String sourceObjectId, String trgObjectId)
            throws FileNotExistsException, FileAlreadyExistsException {
        BlobStore blobStore = connectionProvider.getBlobStore();
        String container = connectionProvider.getContainer();
        if (!blobStore.blobExists(connectionProvider.getContainer(), sourceObjectId)) {
            throw new FileNotExistsException(String.format(MSG_FILE_NOT_EXISTS, sourceObjectId));
        }
        if (blobStore.blobExists(connectionProvider.getContainer(), trgObjectId)) {
            throw new FileAlreadyExistsException(String.format(MSG_TARGET_FILE_ALREADY_EXISTS, trgObjectId));
        }
        Blob blob = blobStore.getBlob(container, sourceObjectId);
        Blob newBlob = blobStore.blobBuilder(trgObjectId).name(trgObjectId).payload(blob.getPayload()).build();
        blobStore.putBlob(container, newBlob);
    }

    @Override
    public void deleteContent(String fileName)
            throws FileNotExistsException {
        BlobStore blobStore = connectionProvider.getBlobStore();
        String container = connectionProvider.getContainer();
        if (!blobStore.blobExists(connectionProvider.getContainer(), fileName)) {
            throw new FileNotExistsException(String.format(MSG_FILE_NOT_EXISTS, fileName));
        }
        blobStore.removeBlob(container, fileName);
    }
}
