package eu.europeana.cloud.service.mcs.config;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.commons.properties.CassandraProperties;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;
import eu.europeana.cloud.service.mcs.Storage;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.persistent.CassandraDataSetService;
import eu.europeana.cloud.service.mcs.persistent.CassandraRecordService;
import eu.europeana.cloud.service.mcs.persistent.DynamicContentProxy;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraContentDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraRecordDAO;
import eu.europeana.cloud.service.mcs.persistent.swift.ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.swift.SimpleSwiftConnectionProvider;
import eu.europeana.cloud.service.mcs.persistent.swift.SwiftConnectionProvider;
import eu.europeana.cloud.service.mcs.persistent.swift.SwiftContentDAO;
import eu.europeana.cloud.service.mcs.properties.SwiftProperties;
import java.util.EnumMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PersistenceConfiguration {

  @Bean
  public SwiftConnectionProvider swiftConnectionProvider(SwiftProperties swiftProperties) {
    return new SimpleSwiftConnectionProvider(
        swiftProperties.getProvider(),
        swiftProperties.getContainer(),
        swiftProperties.getEndpoint(),
        swiftProperties.getUser(),
        swiftProperties.getPassword());
  }

  @Bean
  @Qualifier("mcsCassandraConnectionProvider")
  public CassandraConnectionProvider mcsCassandraProvider() {
    return new CassandraConnectionProvider(
        cassandraMCSProperties().getHosts(),
        cassandraMCSProperties().getPort(),
        cassandraMCSProperties().getKeyspace(),
        cassandraMCSProperties().getUser(),
        cassandraMCSProperties().getPassword());
  }

  @Bean
  @ConfigurationProperties(prefix = "swift")
  protected SwiftProperties swiftProperties() {
    return new SwiftProperties();
  }

  @Bean
  public DynamicContentProxy dynamicContentProxy(ContentDAO swiftContentDAO, ContentDAO cassandraContentDAO) {
    Map<Storage, ContentDAO> params = new EnumMap<>(Storage.class);

    params.put(Storage.OBJECT_STORAGE, swiftContentDAO);
    params.put(Storage.DATA_BASE, cassandraContentDAO);

    return new DynamicContentProxy(params);
  }

  @Bean
  public BucketsHandler bucketsHandler() {
    return new BucketsHandler(mcsCassandraProvider().getSession());
  }

  @Bean
  @ConfigurationProperties(prefix = "cassandra.mcs")
  protected CassandraProperties cassandraMCSProperties() {
    return new CassandraProperties();
  }


  @Bean
  public CassandraRecordService cassandraRecordService(
      CassandraRecordDAO cassandraRecordDAO,
      CassandraDataSetService cassandraDataSetService,
      CassandraDataSetDAO cassandraDataSetDAO,
      DynamicContentProxy dynamicContentProxy,
      UISClientHandler uisClientHandler
  ) {
    return new CassandraRecordService(
        cassandraRecordDAO,
        cassandraDataSetService,
        cassandraDataSetDAO,
        dynamicContentProxy,
        uisClientHandler
    );
  }

  @Bean
  public CassandraDataSetDAO cassandraDataSetDAO(
      @Qualifier("mcsCassandraConnectionProvider") CassandraConnectionProvider cassandraConnectionProvider) {
    return new CassandraDataSetDAO(cassandraConnectionProvider);
  }

  @Bean
  public CassandraDataSetService cassandraDataSetService(
      CassandraDataSetDAO cassandraDataSetDAO,
      CassandraRecordDAO cassandraRecordDAO,
      UISClientHandler uisClientHandler,
      BucketsHandler bucketsHandler) {
    return new CassandraDataSetService(
        cassandraDataSetDAO,
        cassandraRecordDAO,
        uisClientHandler,
        bucketsHandler);
  }

  @Bean
  public CassandraRecordDAO cassandraRecordDAO(
      @Qualifier("mcsCassandraConnectionProvider") CassandraConnectionProvider cassandraConnectionProvider) {
    return new CassandraRecordDAO(cassandraConnectionProvider);
  }

  @Bean
  public ContentDAO cassandraContentDAO(
      @Qualifier("mcsCassandraConnectionProvider") CassandraConnectionProvider cassandraConnectionProvider) {
    return new CassandraContentDAO(cassandraConnectionProvider);
  }

  @Bean
  public ContentDAO swiftContentDAO(SwiftConnectionProvider swiftConnectionProvider) {
    return new SwiftContentDAO(swiftConnectionProvider);
  }
}
