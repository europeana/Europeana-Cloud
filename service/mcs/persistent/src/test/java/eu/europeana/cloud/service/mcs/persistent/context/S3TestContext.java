package eu.europeana.cloud.service.mcs.persistent.context;

import eu.europeana.cloud.service.mcs.persistent.s3.S3ConnectionProvider;
import eu.europeana.cloud.service.mcs.persistent.s3.S3ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.s3.SimpleS3ConnectionProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static eu.europeana.cloud.test.S3TestHelper.S3TestConstants.*;

@Configuration
public class S3TestContext {

  @Bean
  public S3ConnectionProvider connectionProvider() {
    return new SimpleS3ConnectionProvider(
            S3_TEST_CONTAINER,
            S3_TEST_ENDPOINT,
            S3_TEST_USER,
            S3_TEST_PASSWORD,
            S3_TEST_REGION);
  }

  @Bean
  public S3ContentDAO s3ContentDAO() {
    return new S3ContentDAO(connectionProvider());
  }
}
