package eu.europeana.cloud.service.mcs.utils.testcontexts;

import eu.europeana.aas.permission.PermissionsGrantingManager;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.Storage;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.persistent.CassandraDataSetService;
import eu.europeana.cloud.service.mcs.persistent.CassandraRecordService;
import eu.europeana.cloud.service.mcs.persistent.DynamicContentProxy;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraContentDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraDataSetDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraRecordDAO;
import eu.europeana.cloud.service.mcs.persistent.cassandra.CassandraStaticContentDAO;
import eu.europeana.cloud.service.mcs.persistent.s3.ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.s3.S3ContentDAO;
import eu.europeana.cloud.service.mcs.persistent.s3.SimpleS3ConnectionProvider;
import eu.europeana.cloud.service.mcs.utils.DataSetPermissionsVerifier;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.annotation.Order;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.acls.model.MutableAclService;

import java.util.EnumMap;
import java.util.Map;

import static eu.europeana.cloud.test.CassandraTestExtension.JUNIT_AAS_KEYSPACE;
import static eu.europeana.cloud.test.CassandraTestExtension.JUNIT_MCS_KEYSPACE;
import static eu.europeana.cloud.test.S3TestHelper.S3TestConstants.*;

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
  public SimpleS3ConnectionProvider s3ConnectionProvider() {
      return new SimpleS3ConnectionProvider(
              S3_TEST_CONTAINER,
              S3_TEST_ENDPOINT,
              S3_TEST_USER,
              S3_TEST_PASSWORD,
              S3_TEST_REGION);
  }

    @Bean
    @Primary
    public UISClientHandler uisHandler() {
        return Mockito.mock(UISClientHandler.class);
    }

    @Bean
    @Primary
    public MutableAclService mutableAclService() {
        return Mockito.mock(MutableAclService.class);
    }

    @Bean
    @Primary
    public PermissionsGrantingManager permissionsGrantingManager() {
        return Mockito.mock(PermissionsGrantingManager.class);
    }

    @Bean
    @Primary
    public PermissionEvaluator permissionEvaluator() {
        return Mockito.mock(PermissionEvaluator.class);
    }

    @Bean
    public DataSetPermissionsVerifier dataSetPermissionsVerifier(DataSetService dataSetService,
                                                                 PermissionEvaluator permissionEvaluator) {
        return Mockito.mock(DataSetPermissionsVerifier.class);
    }

    @Bean
    public CassandraDataSetService cassandraDataSetService() {
        return Mockito.spy(new CassandraDataSetService(
                cassandraDataSetDAO(),
                cassandraRecordDAO(),
                uisHandler(),
                bucketsHandler()));
  }

  @Bean
  public BucketsHandler bucketsHandler() {
    return new BucketsHandler(dbService().getSession());
  }

  @Bean
  public CassandraDataSetDAO cassandraDataSetDAO() {
    return new CassandraDataSetDAO(dbService());
  }

  @Bean
  public CassandraRecordDAO cassandraRecordDAO() {
    return new CassandraRecordDAO(dbService());
  }

  @Bean
  public S3ContentDAO s3ContentDAO() {
    return new S3ContentDAO(s3ConnectionProvider(), S3_TEST_MAX_PART_SIZE);
  }

  @Bean
  public CassandraContentDAO cassandraContentDAO() {
    return new CassandraContentDAO(dbService());
  }

  @Bean
  public CassandraStaticContentDAO cassandraStaticContentDAO() {
    return new CassandraStaticContentDAO(dbService());
  }

  @Bean
  public DynamicContentProxy dynamicContentProxy() {
    Map<Storage, ContentDAO> params = new EnumMap<>(Storage.class);
    params.put(Storage.DB_STORAGE, cassandraStaticContentDAO());
    params.put(Storage.OBJECT_STORAGE, s3ContentDAO());
    params.put(Storage.DATA_BASE, cassandraContentDAO());

    return new DynamicContentProxy(params);
  }

  @Bean
  public CassandraRecordService cassandraRecordService() {
    return Mockito.spy(new CassandraRecordService(
            cassandraRecordDAO(),
            cassandraDataSetService(),
            cassandraDataSetDAO(),
            dynamicContentProxy(),
            uisHandler())
    );

  }
}
