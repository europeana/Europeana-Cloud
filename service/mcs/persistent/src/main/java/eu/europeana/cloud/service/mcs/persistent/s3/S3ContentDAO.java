package eu.europeana.cloud.service.mcs.persistent.s3;

import eu.europeana.cloud.common.utils.LogMessageCleaner;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

/**
 * Provides DAO operations for S3.
 */
public class S3ContentDAO implements ContentDAO {

  private static final String MSG_FILE_NOT_EXISTS = "File %s not exists";
  private static final String MSG_TARGET_FILE_ALREADY_EXISTS = "Target file %s already exists";
  private static final String S3_OBJECT_OPERATION_LOG_ATTRIBUTE = "s3ObjectOperation";
  private static final String S3_OBJECT_NAME_LOG_ATTRIBUTE = "s3ObjectName";
  @SuppressWarnings("java:S1312")
  private static final Logger LOGGER_S3_MODIFICATIONS = LoggerFactory.getLogger("S3Modifications");
  private static final Logger LOGGER = LoggerFactory.getLogger(S3ContentDAO.class);
  private Integer maxPartSize;

  private final S3ConnectionProvider connectionProvider;

  /**
   * Constructor for S3ContentDAO
   * @param connectionProvider S3ConnectionProvider to be used
   * @param maxPartSize maximum part size for multipart upload
   */
  public S3ContentDAO(S3ConnectionProvider connectionProvider, int maxPartSize) {
    this.connectionProvider = connectionProvider;
    this.maxPartSize = maxPartSize;
  }

  @Override
  public PutResult putContent(String fileName, InputStream data) throws IOException {
    S3Client s3Client = connectionProvider.getS3Client();
    logOperation(fileName, "PUT");
    String container = connectionProvider.getContainer();
    byte[] content = IOUtils.toByteArray(data);
    byte[] md5 = DigestUtils.md5(content);
    String hexMd5 = Hex.encodeHexString(md5);
    Base64.getEncoder().encodeToString(md5);
    CreateMultipartUploadRequest createRequest = CreateMultipartUploadRequest.builder()
            .bucket(container)
            .key(fileName)
            .build();
    CreateMultipartUploadResponse createResponse = s3Client.createMultipartUpload(createRequest);

    String uploadId = createResponse.uploadId();

    List<byte[]> chunks = new ArrayList<>();
    int totalLength = content.length;

    for (int i = 0; i < totalLength; i += maxPartSize) {
      int chunkSize = Math.min(maxPartSize, totalLength - i);
      byte[] chunk = new byte[chunkSize];
      System.arraycopy(content, i, chunk, 0, chunkSize);
      chunks.add(chunk);
    }

    List<CompletedPart> completedParts = new ArrayList<>();
    int partNumber = 1;
    for (byte[] chunk : chunks) {
      long bytesRead = chunk.length;
      String chunkMd5 = Base64.getEncoder().encodeToString(DigestUtils.md5(chunk));
      UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
              .bucket(container)
              .key(fileName)
              .uploadId(uploadId)
              .partNumber(partNumber)
              .contentMD5(chunkMd5)
              .contentLength(bytesRead)
              .build();

      UploadPartResponse response = s3Client.uploadPart(uploadPartRequest, RequestBody.fromBytes(chunk));

      completedParts.add(CompletedPart.builder()
              .partNumber(partNumber)
              .eTag(response.eTag())
              .build());

      partNumber++;

    }

    CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
            .bucket(container)
            .key(fileName)
            .uploadId(uploadId)
            .multipartUpload(builder -> builder.parts(completedParts))
            .build();

    s3Client.completeMultipartUpload(completeRequest);
    return new PutResult(hexMd5, (long) content.length);
  }

  @Override
  public void getContent(String fileName, long start, long end, OutputStream os)
          throws IOException, FileNotExistsException {
    S3Client s3Client = connectionProvider.getS3Client();
    logOperation(fileName, "GET");

    String container = connectionProvider.getContainer();

    try {
      GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
              .bucket(container)
              .key(fileName);

      // Specify byte range if provided
      if (start > -1 && end > -1) {
        requestBuilder.range("bytes=" + start + "-" + end);
      } else if (start > -1) {
        requestBuilder.range("bytes=" + start + "-");
      } else if (end > -1) {
        requestBuilder.range("bytes=0-" + end);
      }

      ResponseInputStream<GetObjectResponse> object = s3Client.getObject(requestBuilder.build());
      IOUtils.copy(object, os);

    } catch (NoSuchKeyException e) {
      LOGGER.debug("File {} not exists, Exception: {}", fileName, e);
      throw new FileNotExistsException(String.format(MSG_FILE_NOT_EXISTS, fileName));
    }
  }

  @Override
  public void copyContent(String sourceObjectId, String trgObjectId)
          throws FileNotExistsException, FileAlreadyExistsException {
    logOperation(trgObjectId, "COPY");

    String container = connectionProvider.getContainer();

    // Check if source exists
    if (!objectExists(container, sourceObjectId)) {
      throw new FileNotExistsException(String.format(MSG_FILE_NOT_EXISTS, sourceObjectId));
    }

    // Check if target already exists
    if (objectExists(container, trgObjectId)) {
      throw new FileAlreadyExistsException(String.format(MSG_TARGET_FILE_ALREADY_EXISTS, trgObjectId));
    }

    CopyObjectRequest copyRequest = CopyObjectRequest.builder()
            .sourceBucket(container)
            .sourceKey(sourceObjectId)
            .destinationBucket(container)
            .destinationKey(trgObjectId)
            .build();

    S3Client s3Client = connectionProvider.getS3Client();
    s3Client.copyObject(copyRequest);
  }

  @Override
  public void deleteContent(String fileName) throws FileNotExistsException {
    logOperation(fileName, "DELETE");

    String container = connectionProvider.getContainer();

    // Check if the file exists
    if (!objectExists(container, fileName)) {
      throw new FileNotExistsException(String.format(MSG_FILE_NOT_EXISTS, fileName));
    }

    DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
            .bucket(container)
            .key(fileName)
            .build();

    S3Client s3Client = connectionProvider.getS3Client();
    s3Client.deleteObject(deleteRequest);
  }

  private boolean objectExists(String bucket, String key) {
    try {
      HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
              .bucket(bucket)
              .key(key)
              .build();

      S3Client s3Client = connectionProvider.getS3Client();
      s3Client.headObject(headObjectRequest);
      return true;
    } catch (NoSuchKeyException e) {
      LOGGER.debug("File {} not exists, Exception: {}", key, e);
      return false;
    }
  }

  private void logOperation(String fileName, String operation) {
    try {
      MDC.put(S3_OBJECT_OPERATION_LOG_ATTRIBUTE, operation);
      MDC.put(S3_OBJECT_NAME_LOG_ATTRIBUTE, fileName);
      if (LOGGER_S3_MODIFICATIONS.isInfoEnabled()) {
        LOGGER_S3_MODIFICATIONS.info("Executed: {} on S3 for file: {}",
                LogMessageCleaner.clean(operation),
                LogMessageCleaner.clean(fileName));
      }
    } finally {
      MDC.remove(S3_OBJECT_OPERATION_LOG_ATTRIBUTE);
      MDC.remove(S3_OBJECT_NAME_LOG_ATTRIBUTE);
    }
  }
}
