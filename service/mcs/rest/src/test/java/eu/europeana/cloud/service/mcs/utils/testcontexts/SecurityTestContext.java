package eu.europeana.cloud.service.mcs.utils.testcontexts;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.persistent.CassandraDataSetService;
import eu.europeana.cloud.service.mcs.persistent.CassandraRecordService;
import eu.europeana.cloud.service.mcs.persistent.swift.SimpleSwiftConnectionProvider;
import eu.europeana.cloud.service.mcs.persistent.uis.UISClientHandlerImpl;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import static eu.europeana.cloud.test.CassandraTestRunner.JUNIT_AAS_KEYSPACE;
import static eu.europeana.cloud.test.CassandraTestRunner.EMBEEDED_CASSANDRA_PORT;
import static eu.europeana.cloud.test.CassandraTestRunner.JUNIT_MCS_KEYSPACE;
import static org.mockito.Mockito.mock;

@Configuration
public class SecurityTestContext {

    @Bean()
    @Order(100)
    public CassandraConnectionProvider aasCassandraProvider() {
        return new CassandraConnectionProvider("localhost", EMBEEDED_CASSANDRA_PORT, JUNIT_AAS_KEYSPACE, "", "");
    }

    @Bean()
    @Order(100)
    public CassandraConnectionProvider dbService() {
        return new CassandraConnectionProvider("localhost", EMBEEDED_CASSANDRA_PORT, JUNIT_MCS_KEYSPACE, "", "");
    }

    @Bean()
    @Order(100)
    public SimpleSwiftConnectionProvider swiftConnectionProvider() {
        return new SimpleSwiftConnectionProvider("transient", "test_container", "", "test_user", "test_pwd");
    }

    //mock
    @Bean
    public UISClientHandler uisHandler() {
        return mock(UISClientHandlerImpl.class);
    }
//
//    @Bean
//    public MutableAclService mutableAclService() {
//        return mock(MutableAclService.class);
//    }

//    @Bean
//    public PermissionsGrantingManager permissionsGrantingManager() {
//        return mock(PermissionsGrantingManager.class);
//    }

    @Bean
    public CassandraDataSetService cassandraDataSetService() {
        return Mockito.mock(CassandraDataSetService.class);
    }

    @Bean
    public CassandraRecordService cassandraRecordService() {
        return Mockito.mock(CassandraRecordService.class);
    }
}
