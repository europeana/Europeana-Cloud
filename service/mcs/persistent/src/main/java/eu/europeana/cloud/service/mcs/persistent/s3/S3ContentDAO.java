package eu.europeana.cloud.service.mcs.persistent.s3;

import com.google.common.hash.HashCode;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.ContainerNotFoundException;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.options.GetOptions;
import org.jclouds.io.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Provides DAO operations for S3.
 */
public class S3ContentDAO implements ContentDAO {

  private static final String MSG_FILE_NOT_EXISTS = "File %s not exists";
  private static final String MSG_TARGET_FILE_ALREADY_EXISTS = "Target file %s already exists";
  private static final String MSG_CANNOT_GET_INSTANCE_OF_MD_5 = "Cannot get instance of MD5 but such algorithm should be provided";
  private static final String S3_OBJECT_OPERATION_LOG_ATTRIBUTE = "s3ObjectOperation";
  private static final String S3_OBJECT_NAME_LOG_ATTRIBUTE = "s3ObjectName";

  @SuppressWarnings("java:S1312") //This is custom logger, so it should have distinguishable name.
  // This name needs to break Sonar logger naming convention.
  private static final Logger S3_MODIFICATIONS_LOGGER = LoggerFactory.getLogger("S3Modifications");

  private final S3ConnectionProvider connectionProvider;

  public S3ContentDAO(S3ConnectionProvider connectionProvider) {
    this.connectionProvider = connectionProvider;
  }

  @Override
  public PutResult putContent(String fileName, InputStream data) throws IOException, ContainerNotFoundException {
    logOperation(fileName, "PUT");
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
    String container = connectionProvider.getContainer();
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
    if (!blobStore.blobExists(connectionProvider.getContainer(), sourceObjectId)) {
      throw new FileNotExistsException(String.format(MSG_FILE_NOT_EXISTS, sourceObjectId));
    }
    if (blobStore.blobExists(connectionProvider.getContainer(), trgObjectId)) {
      throw new FileAlreadyExistsException(String.format(MSG_TARGET_FILE_ALREADY_EXISTS, trgObjectId));
    }
    String container = connectionProvider.getContainer();
    Blob blob = blobStore.getBlob(container, sourceObjectId);
    Blob newBlob = blobStore.blobBuilder(trgObjectId).name(trgObjectId).payload(blob.getPayload()).build();
    blobStore.putBlob(container, newBlob);
  }

  @Override
  public void deleteContent(String fileName)
      throws FileNotExistsException {
    logOperation(fileName, "DELETE");
    BlobStore blobStore = connectionProvider.getBlobStore();
    if (!blobStore.blobExists(connectionProvider.getContainer(), fileName)) {
      throw new FileNotExistsException(String.format(MSG_FILE_NOT_EXISTS, fileName));
    }
    String container = connectionProvider.getContainer();
    blobStore.removeBlob(container, fileName);
  }

  private void logOperation(String fileName, String operation) {
    try {
      MDC.put(S3_OBJECT_OPERATION_LOG_ATTRIBUTE, operation);
      MDC.put(S3_OBJECT_NAME_LOG_ATTRIBUTE, fileName);
      S3_MODIFICATIONS_LOGGER.info("Executed: {} on S3 for file: {}", operation, fileName);
    } finally {
      MDC.remove(S3_OBJECT_OPERATION_LOG_ATTRIBUTE);
      MDC.remove(S3_OBJECT_NAME_LOG_ATTRIBUTE);
    }
  }

  private DigestInputStream md5InputStream(InputStream is) {
    try {
      @SuppressWarnings("java:S4790") //The md5 is used here not for security, but as file checksum,
      // which is part of S3 and eCloud API. The meaningful here is collision probability,
      // which is very low 1.47*10-29. So we could use it here safety.
      MessageDigest md = MessageDigest.getInstance("MD5");
      return new DigestInputStream(is, md);
    } catch (NoSuchAlgorithmException ex) {
      throw new AssertionError(MSG_CANNOT_GET_INSTANCE_OF_MD_5, ex);
    }
  }
}
