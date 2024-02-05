package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import lombok.AllArgsConstructor;
import lombok.Builder;

import javax.annotation.PostConstruct;
import java.io.InputStream;
import java.io.Serializable;

@Builder
@AllArgsConstructor
public class AmazonClient implements Serializable {

  static AmazonS3 amazonS3;
  private final String awsAccessKey;
  private final String awsSecretKey;
  private final String awsEndPoint;
  private final String awsBucket;

  @PostConstruct
  @SuppressWarnings("java:S6263")
  // Credentials are loaded from configuration files that require SSH-key and VPN to access or inner company gitlab account with proper permissions.
  public synchronized void init() {
    if (amazonS3 == null) {
      amazonS3 = new AmazonS3Client(new BasicAWSCredentials(
          awsAccessKey,
          awsSecretKey));
      amazonS3.setEndpoint(awsEndPoint);
    }
  }

  /**
   * Store object in the specified bucket
   *
   * @param bucket bucket name
   * @param name object name
   * @param inputStream object content
   * @param objectMetadata object metadata
   * @return result from AmazonS3
   */
  public PutObjectResult putObject(String bucket, String name, InputStream inputStream, ObjectMetadata objectMetadata) {
    checkInitialized();
    return amazonS3.putObject(bucket, name, inputStream, objectMetadata);
  }

  /**
   * Store object in the default bucket specified during creation of the client
   *
   * @param name object name
   * @param inputStream object content
   * @param objectMetadata object metadata
   * @return result from AmazonS3
   */
  public PutObjectResult putObject(String name, InputStream inputStream, ObjectMetadata objectMetadata) {
    checkInitialized();
    return amazonS3.putObject(awsBucket, name, inputStream, objectMetadata);
  }

  private void checkInitialized() {
    if (amazonS3 == null) {
      throw new RuntimeException("Amazon client is not initialized!");
    }
  }
}
