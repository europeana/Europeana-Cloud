package eu.europeana.cloud.service.mcs.utils.testcontexts;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.mcs.properties.GeneralProperties;
import eu.europeana.cloud.service.mcs.properties.S3Properties;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.mockito.Mockito;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import static eu.europeana.cloud.test.CassandraTestExtension.JUNIT_AAS_KEYSPACE;
import static eu.europeana.cloud.test.CassandraTestExtension.JUNIT_MCS_KEYSPACE;


@TestConfiguration
public class PropertyBeansContext {

    public PropertyBeansContext() {
        CassandraTestInstance
                .getInstance("create_cassandra_schema.cql", JUNIT_MCS_KEYSPACE)
                .initKeyspaceIfNeeded("aas_setup.cql", JUNIT_AAS_KEYSPACE);
    }

    @Bean
    @Primary
    public GeneralProperties generalProperties() {
        return Mockito.mock(GeneralProperties.class);
    }

    @Bean
    @Primary
    public S3Properties s3Properties() {
        return Mockito.mock(S3Properties.class);
    }

    @Bean
    public CassandraProperties cassandraAASProperties() {
        CassandraProperties cassandraProperties = new CassandraProperties();
        cassandraProperties.setHosts("localhost");
        cassandraProperties.setUser("");
        cassandraProperties.setPort(CassandraTestInstance.getPort());
        cassandraProperties.setPassword("");
        cassandraProperties.setKeyspace(JUNIT_AAS_KEYSPACE);
        return cassandraProperties;
  }

  @Bean
  public CassandraProperties cassandraMCSProperties() {
    CassandraProperties cassandraProperties = new CassandraProperties();
    cassandraProperties.setHosts("localhost");
    cassandraProperties.setUser("");
    cassandraProperties.setPort(CassandraTestInstance.getPort());
    cassandraProperties.setPassword("");
    cassandraProperties.setKeyspace(JUNIT_MCS_KEYSPACE);
    return cassandraProperties;
  }
}
