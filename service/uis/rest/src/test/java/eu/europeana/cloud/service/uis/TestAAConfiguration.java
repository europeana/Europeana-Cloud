package eu.europeana.cloud.service.uis;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;
import eu.europeana.cloud.service.uis.dao.CassandraCloudIdDAO;
import eu.europeana.cloud.service.uis.dao.CassandraDataProviderDAO;
import eu.europeana.cloud.service.uis.dao.CassandraLocalIdDAO;
import eu.europeana.cloud.service.uis.service.CassandraDataProviderService;
import eu.europeana.cloud.service.uis.service.CassandraUniqueIdentifierService;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.springframework.context.annotation.Bean;

public class TestAAConfiguration {

    @Bean
    public CassandraConnectionProvider dataProviderDao() {
        return new CassandraConnectionProvider(
                "localhost",
                CassandraTestInstance.getPort(),
                "uis_test4",
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
    public CassandraCloudIdDAO cassandraCloudIdDAO(CassandraConnectionProvider cassandraConnectionProvider) {
        return new CassandraCloudIdDAO(cassandraConnectionProvider);
    }

    @Bean
    public CassandraLocalIdDAO cassandraLocalIdDAO(CassandraConnectionProvider cassandraConnectionProvider) {
        return new CassandraLocalIdDAO(cassandraConnectionProvider);
    }

    @Bean
    public BucketsHandler bucketsHandler(CassandraConnectionProvider cassandraConnectionProvider) {
        return new BucketsHandler(cassandraConnectionProvider.getSession());
    }

    @Bean
    public UniqueIdentifierService uniqueIdentifierService(CassandraCloudIdDAO cassandraCloudIdDAO,
                                                           CassandraLocalIdDAO cassandraLocalIdDAO,
                                                           CassandraDataProviderDAO cassandraDataProviderDAO) {
        return new CassandraUniqueIdentifierService(
                cassandraCloudIdDAO,
                cassandraLocalIdDAO,
                cassandraDataProviderDAO);
    }

}
