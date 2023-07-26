package eu.europeana.cloud.service.mcs.utils.testcontexts;

import static eu.europeana.cloud.test.CassandraTestRunner.JUNIT_AAS_KEYSPACE;
import static eu.europeana.cloud.test.CassandraTestRunner.JUNIT_MCS_KEYSPACE;
import static org.mockito.Mockito.mock;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.persistent.CassandraDataSetService;
import eu.europeana.cloud.service.mcs.persistent.CassandraRecordService;
import eu.europeana.cloud.service.mcs.persistent.s3.SimpleS3ConnectionProvider;
import eu.europeana.cloud.service.mcs.persistent.uis.UISClientHandlerImpl;
import eu.europeana.cloud.service.mcs.utils.DataSetPermissionsVerifier;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.mockito.Mockito;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.annotation.Order;
import org.springframework.security.access.PermissionEvaluator;

@TestConfiguration
public class SecurityTestContext {

  @Bean()
  @Order(100)
  public CassandraConnectionProvider aasCassandraProvider() {
    return new CassandraConnectionProvider("localhost", CassandraTestInstance.getPort(), JUNIT_AAS_KEYSPACE, "", "");
  }

  @Bean()
  @Order(100)
  public CassandraConnectionProvider dbService() {
    return new CassandraConnectionProvider("localhost", CassandraTestInstance.getPort(), JUNIT_MCS_KEYSPACE, "", "");
  }

  @Bean()
  @Order(100)
    public SimpleS3ConnectionProvider s3ConnectionProvider() {
        return new SimpleS3ConnectionProvider("transient", "test_container", "", "test_user", "test_pwd");
  }

  @Bean
  public UISClientHandler uisHandler() {
    return mock(UISClientHandlerImpl.class);
  }

  @Bean
  public CassandraDataSetService cassandraDataSetService() {
    return Mockito.mock(CassandraDataSetService.class);
  }

  @Bean
  public CassandraRecordService cassandraRecordService() {
    return Mockito.mock(CassandraRecordService.class);
  }

  @Bean
  public DataSetPermissionsVerifier dataSetPermissionsVerifier(DataSetService dataSetService,
      PermissionEvaluator permissionEvaluator) {
    return Mockito.mock(DataSetPermissionsVerifier.class);
  }
}
