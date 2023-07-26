package eu.europeana.cloud.service.mcs.persistent.context;

import eu.europeana.cloud.service.mcs.persistent.s3.S3ConnectionProvider;
import eu.europeana.cloud.service.mcs.persistent.s3.S3ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.s3.SimpleS3ConnectionProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class S3TestContext {

  @Bean
  public S3ConnectionProvider connectionProvider() {
    return new SimpleS3ConnectionProvider(
        "transient",
        "test_container",
        "",
        "test_user",
        "test_pwd");
  }

  @Bean
  public S3ContentDAO s3ContentDAO() {
    return new S3ContentDAO(connectionProvider());
  }
}
