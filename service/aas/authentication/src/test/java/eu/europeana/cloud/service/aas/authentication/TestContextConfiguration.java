package eu.europeana.cloud.service.aas.authentication;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.aas.authentication.repository.CassandraUserDAO;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TestContextConfiguration {

    @Bean
    public CassandraConnectionProvider cassandraConnectionProvider(){
        return new CassandraConnectionProvider("localhost", CassandraTestInstance.getPort(), "aas_test", "", "");
    }

    @Bean
    public CassandraAuthenticationService cassandraAuthenticationService(){
        return new CassandraAuthenticationService(cassandraUserDAO());
    }

    @Bean
    public CassandraUserDAO cassandraUserDAO(){
        return new CassandraUserDAO(cassandraConnectionProvider());
    }
}
