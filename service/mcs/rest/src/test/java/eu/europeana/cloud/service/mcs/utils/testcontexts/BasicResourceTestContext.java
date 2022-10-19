package eu.europeana.cloud.service.mcs.utils.testcontexts;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import eu.europeana.aas.acl.ExtendedAclService;
import eu.europeana.aas.acl.PermissionsGrantingManager;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.service.mcs.persistent.CassandraDataSetService;
import eu.europeana.cloud.service.mcs.persistent.CassandraRecordService;
import eu.europeana.cloud.service.mcs.persistent.DynamicContentProxy;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.swift.SimpleSwiftConnectionProvider;
import eu.europeana.cloud.service.mcs.utils.DataSetPermissionsVerifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.acls.AclPermissionEvaluator;

import static org.mockito.Matchers.anyString;
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
    public ExtendedAclService aclService() {
        return mock(ExtendedAclService.class);
    }

    @Bean
    public PermissionsGrantingManager permissionsGrantingManager() {
        return new PermissionsGrantingManager();
    }

    @Bean
    public AclPermissionEvaluator aclPermissionEvaluator() {
        return mock(AclPermissionEvaluator.class);
    }

    @Bean
    public DataSetPermissionsVerifier dataSetPermissionsVerifier(){
        return mock(DataSetPermissionsVerifier.class);
    }

    @Bean
    public CassandraDataSetDAO cassandraDataSetDAO() {
        return mock(CassandraDataSetDAO.class);
    }

    @Bean
    public DynamicContentProxy dynamicContentProxy() {
        return mock(DynamicContentProxy.class);
    }

    @Bean
    public CassandraDataSetService cassandraDataSetService() {
        return new CassandraDataSetService();
    }

    @Bean
    public SimpleSwiftConnectionProvider swiftConnectionProvider() {
        return mock(SimpleSwiftConnectionProvider.class);
    }

    @Bean
    public CassandraRecordService cassandraRecordService() {
        return mock(CassandraRecordService.class);
    }

    @Bean
    public UISClient uisClient(){
        return mock(UISClient.class);
    }
}
