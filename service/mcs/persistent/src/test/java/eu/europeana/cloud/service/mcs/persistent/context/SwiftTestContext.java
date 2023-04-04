package eu.europeana.cloud.service.mcs.persistent.context;

import eu.europeana.cloud.service.mcs.persistent.swift.SimpleSwiftConnectionProvider;
import eu.europeana.cloud.service.mcs.persistent.swift.SwiftConnectionProvider;
import eu.europeana.cloud.service.mcs.persistent.swift.SwiftContentDAO;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@TestConfiguration
public class SwiftTestContext {

  @Bean
  public SwiftConnectionProvider connectionProvider() {
    return new SimpleSwiftConnectionProvider(
        "transient",
        "test_container",
        "",
        "test_user",
        "test_pwd");
  }

  @Bean
  public SwiftContentDAO swiftContentDAO() {
    return new SwiftContentDAO(connectionProvider());
  }
}
