package eu.europeana.cloud.service.uis;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.uis.config.UnifiedExceptionsMapper;
import eu.europeana.cloud.service.uis.dao.CassandraDataProviderDAO;
import eu.europeana.cloud.service.uis.rest.DataProviderActivationResource;
import eu.europeana.cloud.service.uis.rest.DataProviderResource;
import eu.europeana.cloud.service.uis.rest.DataProvidersResource;
import eu.europeana.cloud.service.uis.rest.UniqueIdentifierResource;
import eu.europeana.cloud.service.uis.service.CassandraDataProviderService;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@EnableWebMvc
@Import({UnifiedExceptionsMapper.class})
public class TestConfiguration {

  @Bean
  public DataProviderActivationResource dataProviderActivationResource(DataProviderService providerService) {
    return new DataProviderActivationResource(providerService);
  }

  @Bean
  public DataProviderResource dataProviderResource(UniqueIdentifierService uniqueIdentifierService,
      DataProviderService providerService) {
    return new DataProviderResource(
        uniqueIdentifierService,
        providerService);
  }

  @Bean
  public UniqueIdentifierResource uniqueIdentifierResource(UniqueIdentifierService uniqueIdentifierService,
      DataProviderResource dataProviderResource,
      ACLServiceWrapper aclWrapper) {
    return new UniqueIdentifierResource(uniqueIdentifierService);
  }

  @Bean
  public UniqueIdentifierService uniqueIdentifierService() {
    return Mockito.mock(UniqueIdentifierService.class);
  }

  @Bean
  public DataProvidersResource dataProvidersResource(DataProviderService providerService,
      ACLServiceWrapper aclWrapper) {
    return new DataProvidersResource(providerService);
  }

  @Bean
  public UnifiedExceptionsMapper unifiedExceptionsMapper() {
    return new UnifiedExceptionsMapper();
  }

  @Bean
  public CassandraDataProviderService cassandraDataProviderService() {
    return Mockito.mock(CassandraDataProviderService.class);
  }

  @Bean
  public CassandraDataProviderDAO cassandraDataProviderDAO(CassandraConnectionProvider cassandraConnectionProvider) {
    return Mockito.mock(CassandraDataProviderDAO.class);
  }

  @Bean
  public CassandraConnectionProvider cassandraConnectionProvider() {
    return Mockito.mock(CassandraConnectionProvider.class);
  }

  @Bean
  public ACLServiceWrapper aclServiceWrapper() {
    return Mockito.mock(ACLServiceWrapper.class);
  }
}
