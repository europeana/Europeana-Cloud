package eu.europeana.cloud.service.uis;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;
import eu.europeana.cloud.service.uis.dao.CassandraDataProviderDAO;
import eu.europeana.cloud.service.uis.dao.CloudIdDAO;
import eu.europeana.cloud.service.uis.dao.CloudIdLocalIdBatches;
import eu.europeana.cloud.service.uis.dao.LocalIdDAO;
import eu.europeana.cloud.service.uis.service.CassandraDataProviderService;
import eu.europeana.cloud.service.uis.service.UniqueIdentifierServiceImpl;
import eu.europeana.cloud.test.CassandraEnvironment;
import org.springframework.context.annotation.Bean;

public class TestAAConfiguration {

  @Bean
  public CassandraConnectionProvider dataProviderDao() {
    return new CassandraConnectionProvider(
            CassandraEnvironment.HOST, CassandraEnvironment.getPort(),
            "uis_test" + CassandraEnvironment.KEYSPACE_SUFFIX,
        "",
        "");
  }

  @Bean
  public CassandraDataProviderService cassandraDataProviderService(CassandraDataProviderDAO dataProviderDAO) {
    return new CassandraDataProviderService(dataProviderDAO);
  }

  @Bean
  public CassandraDataProviderDAO cassandraDataProviderDAO(CassandraConnectionProvider cassandraConnectionProvider) {
    return new CassandraDataProviderDAO(cassandraConnectionProvider);
  }

  @Bean
  public CloudIdDAO cassandraCloudIdDAO(CassandraConnectionProvider cassandraConnectionProvider) {
    return new CloudIdDAO(cassandraConnectionProvider);
  }

  @Bean
  public LocalIdDAO cassandraLocalIdDAO(CassandraConnectionProvider cassandraConnectionProvider) {
    return new LocalIdDAO(cassandraConnectionProvider);
  }

  @Bean
  public BucketsHandler bucketsHandler(CassandraConnectionProvider cassandraConnectionProvider) {
    return new BucketsHandler(cassandraConnectionProvider.getSession());
  }

  @Bean
  public CloudIdLocalIdBatches cloudIdLocalIdBatches(CloudIdDAO cloudIdDAO, LocalIdDAO localIdDAO,
      CassandraConnectionProvider cassandraConnectionProvider) {
    return new CloudIdLocalIdBatches(cloudIdDAO, localIdDAO, cassandraConnectionProvider);
  }

  @Bean
  public eu.europeana.cloud.service.uis.UniqueIdentifierService uniqueIdentifierService(CloudIdDAO cassandraCloudIdDAO,
      LocalIdDAO cassandraLocalIdDAO,
      CassandraDataProviderDAO cassandraDataProviderDAO,
      CloudIdLocalIdBatches cloudIdLocalIdBatches) {
    return new UniqueIdentifierServiceImpl(
        cassandraCloudIdDAO,
        cassandraLocalIdDAO,
        cassandraDataProviderDAO,
        cloudIdLocalIdBatches);
  }

}
