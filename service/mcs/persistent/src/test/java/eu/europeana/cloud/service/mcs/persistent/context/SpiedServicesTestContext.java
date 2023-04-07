package eu.europeana.cloud.service.mcs.persistent.context;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

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
import eu.europeana.cloud.service.mcs.persistent.swift.ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.swift.SimpleSwiftConnectionProvider;
import eu.europeana.cloud.service.mcs.persistent.swift.SwiftContentDAO;
import eu.europeana.cloud.test.CassandraTestInstance;
import java.util.EnumMap;
import java.util.Map;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SpiedServicesTestContext {

  @Bean
  public CassandraConnectionProvider dbService() {
    return spy(new CassandraConnectionProvider("localhost", CassandraTestInstance.getPort(), "junit_mcs", "", ""));
  }

  @Bean
  public CassandraDataSetDAO cassandraDataSetDAO() {
    return spy(new CassandraDataSetDAO(dbService()));
  }

  @Bean
  public CassandraRecordService cassandraRecordService() {
    return spy(new CassandraRecordService(
        cassandraRecordDAO(),
        cassandraDataSetService(),
        cassandraDataSetDAO(),
        dynamicContentDAO(),
        uisClientHandler()
    ));
  }

  @Bean
  public CassandraRecordDAO cassandraRecordDAO() {
    return spy(new CassandraRecordDAO(dbService()));
  }

  @Bean
  public CassandraContentDAO cassandraContentDAO() {
    return spy(new CassandraContentDAO(dbService()));
  }

  @Bean
  public SimpleSwiftConnectionProvider simpleSwiftConnectionProvider() {
    return spy(new SimpleSwiftConnectionProvider(
        "transient",
        "test_container",
        "", "test_user",
        "test_pwd"));
  }

  @Bean
  public BucketsHandler bucketsHandler() {
    return spy(new BucketsHandler(dbService().getSession()));
  }

  @Bean
  public SwiftContentDAO swiftContentDAO() {
    return spy(new SwiftContentDAO(simpleSwiftConnectionProvider()));
  }

  @Bean
  public UISClientHandler uisClientHandler() {
    return mock(UISClientHandler.class);
  }


  @Bean
  public DynamicContentProxy dynamicContentDAO() {
    Map<Storage, ContentDAO> params = new EnumMap<>(Storage.class);

    params.put(Storage.OBJECT_STORAGE, swiftContentDAO());
    params.put(Storage.DATA_BASE, cassandraContentDAO());

    return spy(new DynamicContentProxy(params));
  }

  @Bean
  public CassandraDataSetService cassandraDataSetService() {
    return spy(new CassandraDataSetService(
        cassandraDataSetDAO(),
        cassandraRecordDAO(),
        uisClientHandler(),
        bucketsHandler()
    ));
  }

}
