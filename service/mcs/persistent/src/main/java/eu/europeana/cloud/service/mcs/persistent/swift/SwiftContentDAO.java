package eu.europeana.cloud.service.mcs.persistent.swift;

import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingInputStream;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.ContainerNotFoundException;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.io.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Provides DAO operations for Openstack Swift.
 */
@Repository
public class SwiftContentDAO implements ContentDAO {

    private static final String MSG_FILE_NOT_EXISTS = "File %s not exists";
    private static final String MSG_TARGET_FILE_ALREADY_EXISTS = "Target file %s already exists";
    private static final String MSG_CANNOT_GET_INSTANCE_OF_MD_5 = "Cannot get instance of MD5 but such algorithm should be provided";
    private static final String SWIFT_OBJECT_OPERATION_LOG_ATTRIBUTE = "swiftObjectOperation";
    private static final String SWIFT_OBJECT_NAME_LOG_ATTRIBUTE = "swiftObjectName";

    @SuppressWarnings("java:S1312") //This is custom logger, so it should have distinguishable name.
    // This name needs to break Sonar logger naming convention.
    private static final Logger SWIFT_MODIFICATIONS_LOGGER = LoggerFactory.getLogger("SwiftModifications");

    @Autowired
    private SwiftConnectionProvider connectionProvider;

    @Override
    public PutResult putContent(String fileName, InputStream data) throws IOException, ContainerNotFoundException {
        logOperation(fileName, "PUT");
        BlobStore blobStore = connectionProvider.getBlobStore();
        String container = connectionProvider.getContainer();
        CountingInputStream countingInputStream = new CountingInputStream(data);
        DigestInputStream md5DigestInputStream = md5InputStream(countingInputStream);
        BlobBuilder builder = blobStore.blobBuilder(fileName);
        builder = builder.name(fileName);
        builder = builder.payload(md5DigestInputStream);
        Blob blob = builder.build();
        blobStore.putBlob(container, blob);

        String md5 = BaseEncoding.base16().lowerCase().encode(md5DigestInputStream.getMessageDigest().digest());
        Long contentLength = countingInputStream.getCount();
        return new PutResult(md5, contentLength);
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
        logOperation(trgObjectId, "COPY");
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
        logOperation(fileName, "DELETE");
        BlobStore blobStore = connectionProvider.getBlobStore();
        String container = connectionProvider.getContainer();
        if (!blobStore.blobExists(connectionProvider.getContainer(), fileName)) {
            throw new FileNotExistsException(String.format(MSG_FILE_NOT_EXISTS, fileName));
        }
        blobStore.removeBlob(container, fileName);
    }

    private void logOperation(String fileName, String operation) {
        try {
            MDC.put(SWIFT_OBJECT_OPERATION_LOG_ATTRIBUTE, operation);
            MDC.put(SWIFT_OBJECT_NAME_LOG_ATTRIBUTE, fileName);
            SWIFT_MODIFICATIONS_LOGGER.info("Executed: {} on Swift for file: {}", operation, fileName);
        } finally {
            MDC.remove(SWIFT_OBJECT_OPERATION_LOG_ATTRIBUTE);
            MDC.remove(SWIFT_OBJECT_NAME_LOG_ATTRIBUTE);
        }
    }

    private DigestInputStream md5InputStream(InputStream is) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return new DigestInputStream(is, md);
        } catch (NoSuchAlgorithmException ex) {
            throw new AssertionError(MSG_CANNOT_GET_INSTANCE_OF_MD_5, ex);
        }
    }
}
