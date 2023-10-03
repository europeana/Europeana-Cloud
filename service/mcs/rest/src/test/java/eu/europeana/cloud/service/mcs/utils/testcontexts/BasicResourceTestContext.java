package eu.europeana.cloud.service.mcs.utils.testcontexts;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.security.acls.AclPermissionEvaluator;

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

  @MockBean
  public ExtendedAclService aclService;

  @Bean
  public PermissionsGrantingManager permissionsGrantingManager() {
    return new PermissionsGrantingManager();
  }

  @MockBean
  public AclPermissionEvaluator aclPermissionEvaluator;

  @MockBean
  public DataSetPermissionsVerifier dataSetPermissionsVerifier;

  @MockBean
  public CassandraDataSetDAO cassandraDataSetDAO;

  @MockBean
  public DynamicContentProxy dynamicContentProxy;
  @MockBean
  public SimpleS3ConnectionProvider s3ConnectionProvider;
  @MockBean
  public CassandraRecordService cassandraRecordService;
  @MockBean
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
