package eu.europeana.cloud.service.mcs.persistent.s3;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.net.URI;

public class SimpleS3ConnectionProvider implements S3ConnectionProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleS3ConnectionProvider.class);
  private S3Client s3Client;
  private final String container;
  private final String region;
  private final AwsBasicCredentials awsCreds;
  private final String endpoint;

  /**
   * Class constructor. Establish connection to S3 endpoint using provided configuration.
   *
   * @param container name of the S3 bucket (namespace)
   * @param endpoint S3 endpoint URL (optional for AWS S3, but can be used for custom S3-compatible services)
   * @param user access key
   * @param password secret key
   */
  public SimpleS3ConnectionProvider(String container, String endpoint, String user, String password, String region) {

    this.awsCreds = AwsBasicCredentials.create(user, password);
    this.endpoint = endpoint;
    this.container = container;
    this.region = region;
    openConnections();
  }

  private void openConnections() {
    try {
      this.s3Client = S3Client.builder()
              .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
              .endpointOverride(URI.create(endpoint))
              .serviceConfiguration(S3Configuration.builder()
                      .pathStyleAccessEnabled(true)
                      .build())
              .region(Region.of(region))
              .build();
      LOGGER.info("Connected to S3 bucket: {}", container);
  } catch (S3Exception e) {
      LOGGER.error("Error connecting to S3: {}", e.awsErrorDetails().errorMessage());
      throw e;

    }
  }

  @Override
  @PreDestroy
  public void closeConnections() {
    LOGGER.info("Shutting down S3 client connection");
    s3Client.close();
  }

  @Override
  public String getContainer() {
    return container;
  }

  public S3Client getS3Client() {
    return s3Client;
  }
}
