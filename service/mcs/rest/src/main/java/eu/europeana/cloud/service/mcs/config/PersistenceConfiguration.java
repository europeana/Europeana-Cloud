package eu.europeana.cloud.service.mcs.config;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;
import eu.europeana.cloud.service.mcs.Storage;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.persistent.CassandraDataSetService;
import eu.europeana.cloud.service.mcs.persistent.CassandraRecordService;
import eu.europeana.cloud.service.mcs.persistent.DynamicContentProxy;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraContentDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraRecordDAO;
import eu.europeana.cloud.service.mcs.persistent.s3.ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.s3.S3ConnectionProvider;
import eu.europeana.cloud.service.mcs.persistent.s3.S3ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.s3.SimpleS3ConnectionProvider;
import eu.europeana.cloud.service.mcs.properties.S3Properties;
import eu.europeana.cloud.common.properties.CassandraProperties;
import java.util.EnumMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PersistenceConfiguration {

  @Bean
  S3ConnectionProvider s3ConnectionProvider(S3Properties s3Properties) {
    return new SimpleS3ConnectionProvider(
        s3Properties.getProvider(),
        s3Properties.getContainer(),
        s3Properties.getEndpoint(),
        s3Properties.getUser(),
        s3Properties.getPassword()) {
    };
  }

  @Bean("mcsCassandraConnectionProvider")
  CassandraConnectionProvider mcsCassandraProvider() {
    return new CassandraConnectionProvider(
        cassandraMCSProperties().getHosts(),
        cassandraMCSProperties().getPort(),
        cassandraMCSProperties().getKeyspace(),
        cassandraMCSProperties().getUser(),
        cassandraMCSProperties().getPassword());
  }

  @Bean
  @ConfigurationProperties(prefix = "s3")
  S3Properties s3Properties() {
    return new S3Properties();
  }

  @Bean
  DynamicContentProxy dynamicContentProxy(
          @Qualifier("s3ContentDAO") ContentDAO s3ContentDAO,
          @Qualifier("cassandraContentDAO") ContentDAO cassandraContentDAO) {
    Map<Storage, ContentDAO> params = new EnumMap<>(Storage.class);

    params.put(Storage.OBJECT_STORAGE, s3ContentDAO);
    params.put(Storage.DATA_BASE, cassandraContentDAO);

    return new DynamicContentProxy(params);
  }

  @Bean
  BucketsHandler bucketsHandler() {
    return new BucketsHandler(mcsCassandraProvider().getSession());
  }

  @Bean
  @ConfigurationProperties(prefix = "cassandra.mcs")
  CassandraProperties cassandraMCSProperties() {
    return new CassandraProperties();
  }


  @Bean
  CassandraRecordService cassandraRecordService(
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
  CassandraDataSetDAO cassandraDataSetDAO(
      @Qualifier("mcsCassandraConnectionProvider") CassandraConnectionProvider cassandraConnectionProvider) {
    return new CassandraDataSetDAO(cassandraConnectionProvider);
  }

  @Bean
  CassandraDataSetService cassandraDataSetService(
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
  CassandraRecordDAO cassandraRecordDAO(
      @Qualifier("mcsCassandraConnectionProvider") CassandraConnectionProvider cassandraConnectionProvider) {
    return new CassandraRecordDAO(cassandraConnectionProvider);
  }

  @Bean
  ContentDAO cassandraContentDAO(
      @Qualifier("mcsCassandraConnectionProvider") CassandraConnectionProvider cassandraConnectionProvider) {
    return new CassandraContentDAO(cassandraConnectionProvider);
  }

  @Bean
  ContentDAO s3ContentDAO(S3ConnectionProvider s3ConnectionProvider) {
    return new S3ContentDAO(s3ConnectionProvider);
  }
}
