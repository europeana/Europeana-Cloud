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
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.security.acls.AclPermissionEvaluator;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestConfiguration
public class BasicResourceTestContext {

  @Bean()
  public CassandraConnectionProvider dbService() {
    CassandraConnectionProvider dbService = mock(CassandraConnectionProvider.class);
    Session session = mock(Session.class);
    when(session.prepare(anyString())).thenReturn(mock(PreparedStatement.class));
    when(dbService.getSession()).thenReturn(session);
    return dbService;
  }

  @MockitoBean
  public ExtendedAclService aclService;

  @Bean
  public PermissionsGrantingManager permissionsGrantingManager() {
    return new PermissionsGrantingManager();
  }

  @MockitoBean
  public AclPermissionEvaluator aclPermissionEvaluator;

  @MockitoBean
  public DataSetPermissionsVerifier dataSetPermissionsVerifier;

  @MockitoBean
  public CassandraDataSetDAO cassandraDataSetDAO;
  @MockitoBean
  public CassandraRecordDAO cassandraRecordDAO;

  @MockitoBean
  public DynamicContentProxy dynamicContentProxy;
  @MockitoBean
  public SimpleS3ConnectionProvider s3ConnectionProvider;
  @MockitoBean
  public CassandraRecordService cassandraRecordService;
  @MockitoBean
  public UISClient uisClient;

  @Bean
  public CassandraDataSetService cassandraDataSetService() {
    return new CassandraDataSetService(
        new CassandraDataSetDAO(dbService()),
        new CassandraRecordDAO(dbService()),
        mock(UISClientHandler.class),
        new BucketsHandler(dbService().getSession()));
  }
}
