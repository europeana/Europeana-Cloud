package eu.europeana.cloud.service.mcs.utils.testcontexts;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import eu.europeana.aas.authorization.ExtendedAclService;
import eu.europeana.aas.permission.PermissionsGrantingManager;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.persistent.CassandraDataSetService;
import eu.europeana.cloud.service.mcs.persistent.CassandraRecordService;
import eu.europeana.cloud.service.mcs.persistent.DynamicContentProxy;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraRecordDAO;
import eu.europeana.cloud.service.mcs.persistent.s3.SimpleS3ConnectionProvider;
import eu.europeana.cloud.service.mcs.utils.DataSetPermissionsVerifier;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.security.acls.AclPermissionEvaluator;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Configuration
public class BasicResourceTestContext {

  @Bean()
  public CassandraConnectionProvider dbService() {
    CassandraConnectionProvider dbService = mock(CassandraConnectionProvider.class);
    Session session = mock(Session.class);
    when(session.prepare(anyString())).thenReturn(mock(PreparedStatement.class));
    when(dbService.getSession()).thenReturn(session);
    return dbService;
  }

  @Bean
  public PermissionsGrantingManager permissionsGrantingManager() {
    return new PermissionsGrantingManager();
  }

  @Bean
  @Primary
  public ExtendedAclService aclService() {
    return Mockito.mock(ExtendedAclService.class);
  }

  @Bean
  @Primary
  public AclPermissionEvaluator aclPermissionEvaluator() {
    return Mockito.mock(AclPermissionEvaluator.class);
  }

  @Bean
  @Primary
  public DataSetPermissionsVerifier dataSetPermissionsVerifier() {
    return Mockito.mock(DataSetPermissionsVerifier.class);
  }

  @Bean
  @Primary
  public CassandraRecordDAO cassandraRecordDAO() {
    return Mockito.mock(CassandraRecordDAO.class);
  }

  @Bean
  @Primary
  public CassandraDataSetDAO cassandraDataSetDAO() {
    return Mockito.mock(CassandraDataSetDAO.class);
  }

  @Bean
  @Primary
  public DynamicContentProxy dynamicContentProxy() {
    return Mockito.mock(DynamicContentProxy.class);
  }

  @Bean
  @Primary
  public SimpleS3ConnectionProvider s3ConnectionProvider() {
    return Mockito.mock(SimpleS3ConnectionProvider.class);
  }

  @Bean
  @Primary
  public CassandraRecordService cassandraRecordService() {
    return Mockito.mock(CassandraRecordService.class);
  }

  @Bean
  @Primary
  public UISClient uisClient() {
    return Mockito.mock(UISClient.class);
  }

  @Bean
  public CassandraDataSetService cassandraDataSetService() {
    return new CassandraDataSetService(
            new CassandraDataSetDAO(dbService()),
            new CassandraRecordDAO(dbService()),
            mock(UISClientHandler.class),
            new BucketsHandler(dbService().getSession()));
  }
}
