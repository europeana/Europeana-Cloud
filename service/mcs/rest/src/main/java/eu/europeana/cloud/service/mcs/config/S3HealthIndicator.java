package eu.europeana.cloud.service.mcs.config;

import com.datastax.driver.core.exceptions.DriverException;
import eu.europeana.cloud.service.mcs.persistent.s3.S3ConnectionProvider;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;

/**
 * Verifies S3 connection for Actuator health endpoint
 */
public class S3HealthIndicator implements HealthIndicator {

  private static final Log LOGGER = LogFactory.getLog(S3HealthIndicator.class);

  private final S3ConnectionProvider s3ConnectionProvider;

  /**
   * Constructor for component.
   * @param s3ConnectionProvider s3ConnectionProvider
   */
  public S3HealthIndicator(S3ConnectionProvider s3ConnectionProvider) {
    this.s3ConnectionProvider = s3ConnectionProvider;
  }

  @Override
  public Health health() {
    try {
      LOGGER.debug("Verifying S3 connection");
      s3ConnectionProvider.getS3Client().listBuckets();
      return Health.up()
              .withDetail("bucket", s3ConnectionProvider.getS3Client().listBuckets().buckets().getFirst().name())
              .build();

    } catch (DriverException e) {
      LOGGER.error("Error while verifying S3 connection", e);
      return Health.down()
              .build();
    }
  }
}
