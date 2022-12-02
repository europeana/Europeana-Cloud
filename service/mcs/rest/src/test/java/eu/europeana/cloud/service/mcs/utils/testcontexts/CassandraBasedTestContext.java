package eu.europeana.cloud.service.mcs.utils.testcontexts;

import static eu.europeana.cloud.test.CassandraTestRunner.JUNIT_AAS_KEYSPACE;
import static eu.europeana.cloud.test.CassandraTestRunner.JUNIT_MCS_KEYSPACE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import eu.europeana.aas.authorization.ExtendedAclService;
import eu.europeana.aas.permission.PermissionsGrantingManager;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.commons.utils.RetryAspect;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.persistent.CassandraDataSetService;
import eu.europeana.cloud.service.mcs.persistent.CassandraRecordService;
import eu.europeana.cloud.service.mcs.persistent.swift.SimpleSwiftConnectionProvider;
import eu.europeana.cloud.service.mcs.persistent.uis.UISClientHandlerImpl;
import eu.europeana.cloud.service.mcs.utils.DataSetPermissionsVerifier;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.annotation.Order;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.acls.model.MutableAclService;

@Configuration
public class CassandraBasedTestContext {

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
  public SimpleSwiftConnectionProvider swiftConnectionProvider() {
    return new SimpleSwiftConnectionProvider("transient", "test_container", "", "test_user", "test_pwd");
  }

  //mock
  @Bean
  public UISClientHandler uisHandler() {
    return mock(UISClientHandlerImpl.class);
  }

  @Bean
  public MutableAclService mutableAclService() {
    return mock(ExtendedAclService.class);
  }

  @Bean
  public PermissionsGrantingManager permissionsGrantingManager() {
    return mock(PermissionsGrantingManager.class);
  }

  @Bean
  public PermissionEvaluator permissionEvaluator() {
    return mock(PermissionEvaluator.class);
  }

  @Bean
  public CassandraDataSetService cassandraDataSetService() {
    return spy(new CassandraDataSetService());
  }

  @Bean
  public CassandraRecordService cassandraRecordService() {
    return spy(new CassandraRecordService());
  }

  @Bean
  @Primary
  public RetryAspect retryAspect() {
    return spy(new RetryAspect());
  }

  @Bean
  public DataSetPermissionsVerifier dataSetPermissionsVerifier(DataSetService dataSetService,
      PermissionEvaluator permissionEvaluator) {
    return Mockito.mock(DataSetPermissionsVerifier.class);
  }
}
