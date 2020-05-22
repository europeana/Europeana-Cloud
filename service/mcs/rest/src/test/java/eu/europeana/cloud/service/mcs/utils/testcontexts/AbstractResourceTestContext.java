package eu.europeana.cloud.service.mcs.utils.testcontexts;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import eu.europeana.aas.acl.CassandraMutableAclService;
import eu.europeana.aas.acl.repository.CassandraAclRepository;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.commons.permissions.PermissionsGrantingManager;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.Storage;
import eu.europeana.cloud.service.mcs.persistent.CassandraDataSetService;
import eu.europeana.cloud.service.mcs.persistent.CassandraRecordService;
import eu.europeana.cloud.service.mcs.persistent.DynamicContentDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.swift.ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.swift.SimpleSwiftConnectionProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.acls.AclPermissionEvaluator;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Configuration
public class AbstractResourceTestContext {
//
//    @Bean()
//    @Order(100)
//    public SimpleSwiftConnectionProvider swiftConnectionProvider() {
//        return new SimpleSwiftConnectionProvider("transient", "test_container", "", "test_user", "test_pwd");
//    }

//    //mock
    @Bean
    public CassandraRecordService cassandraRecordService() {
        return mock(CassandraRecordService.class);
    }
//
//    @Bean
//    public MutableAclService mutableAclService() {
//        return mock(MutableAclService.class);
//    }
//
//    @Bean
//    public PermissionsGrantingManager permissionsGrantingManager() {
//        return mock(PermissionsGrantingManager.class);
//    }
//
//    @Bean
//    public CassandraDataSetService cassandraDataSetService() {
//        return Mockito.spy(new CassandraDataSetService());
//    }
//
//    @Bean
//    public CassandraRecordService cassandraRecordService() {
//        return Mockito.spy(new CassandraRecordService());
//    }

    @Bean()
    //@Order(100)
    public CassandraConnectionProvider dbService() {
        CassandraConnectionProvider dbService = mock(CassandraConnectionProvider.class);
        Session session = mock(Session.class);
        when(session.prepare(anyString())).thenReturn(mock(PreparedStatement.class));
        when(dbService.getSession()).thenReturn(session);
        return dbService;
    }


    @Bean
    public CassandraMutableAclService aclService() {
        return mock(CassandraMutableAclService.class);
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
    public CassandraDataSetDAO cassandraDataSetDAO() {
        return mock(CassandraDataSetDAO.class);
    }

    @Bean
    public DynamicContentDAO dynamicContentDAO() {
        return mock(DynamicContentDAO.class);
    }

    @Bean
    public CassandraDataSetService cassandraDataSetService() {
        return new CassandraDataSetService();
    }

    @Bean
    public SimpleSwiftConnectionProvider swiftConnectionProvider() {
        return mock(SimpleSwiftConnectionProvider.class);
    }
}
