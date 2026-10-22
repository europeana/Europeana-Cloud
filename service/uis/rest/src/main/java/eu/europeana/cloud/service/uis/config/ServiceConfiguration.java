package eu.europeana.cloud.service.uis.config;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.commons.CassandraHealthIndicator;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;
import eu.europeana.cloud.service.commons.utils.RetryAspect;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.dao.CassandraDataProviderDAO;
import eu.europeana.cloud.service.uis.dao.CloudIdDAO;
import eu.europeana.cloud.service.uis.dao.CloudIdLocalIdBatches;
import eu.europeana.cloud.service.uis.dao.LocalIdDAO;
import eu.europeana.cloud.service.uis.service.CassandraDataProviderService;
import eu.europeana.cloud.service.uis.service.UniqueIdentifierServiceImpl;
import eu.europeana.cloud.service.web.common.LoggingFilter;
import io.micrometer.core.aop.CountedAspect;
import io.micrometer.core.aop.TimedAspect;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.MethodInvokingFactoryBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.micrometer.metrics.autoconfigure.MeterRegistryCustomizer;
import org.springframework.context.annotation.*;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;


/**
 * Spring configuration class for the UIS application
 */
@Configuration
@EnableWebMvc
@ComponentScan("eu.europeana.cloud.service.uis")
@EnableAspectJAutoProxy
@PropertySource(value = "classpath:uis.properties", ignoreResourceNotFound = true)
public class ServiceConfiguration implements WebMvcConfigurer {

  @Bean
  @ConfigurationProperties(prefix = "cassandra.aas")
  CassandraProperties aasCassandraProperties() {
    return new CassandraProperties();
  }

  @Bean
  @ConfigurationProperties(prefix = "cassandra.uis")
  CassandraProperties uisCassandraProperties(){
    return new CassandraProperties();
  }

  @Bean
  UniqueIdentifierService uniqueIdentifierService(
          CloudIdDAO cassandraCloudIdDAO,
          LocalIdDAO cassandraLocalIdDAO,
          CassandraDataProviderDAO cassandraDataProviderDAO,
          CloudIdLocalIdBatches cloudIdLocalIdBatches) {

    return new UniqueIdentifierServiceImpl(
            cassandraCloudIdDAO,
            cassandraLocalIdDAO,
            cassandraDataProviderDAO,
            cloudIdLocalIdBatches);
  }

  @Bean
  CloudIdDAO cloudIdDAO(CassandraConnectionProvider uisCassandraProvider) {
    return new CloudIdDAO(uisCassandraProvider);
  }

  @Bean
  LocalIdDAO localIdDAO(CassandraConnectionProvider uisCassandraProvider) {
    return new LocalIdDAO(uisCassandraProvider);
  }

  @Bean
  CloudIdLocalIdBatches cloudIdLocalIdBatches(CloudIdDAO cloudIdDAO, LocalIdDAO localIdDAO,
                                                     CassandraConnectionProvider uisCassandraProvider) {
    return new CloudIdLocalIdBatches(cloudIdDAO, localIdDAO, uisCassandraProvider);
  }

  @Bean
  CassandraDataProviderService cassandraDataProviderService(CassandraDataProviderDAO dataProviderDAO) {
    return new CassandraDataProviderService(dataProviderDAO);
  }

  @Bean
  CassandraDataProviderDAO cassandraDataProviderDAO(CassandraProperties uisCassandraProperties) {
    return new CassandraDataProviderDAO(new CassandraConnectionProvider(
            uisCassandraProperties.getHosts(),
            uisCassandraProperties.getPort(),
            uisCassandraProperties.getKeyspace(),
            uisCassandraProperties.getUser(),
            uisCassandraProperties.getPassword()));
  }

  @Bean("aasCassandraProvider")
  CassandraConnectionProvider aasCassandraProvider(
      @Qualifier("aasCassandraProperties") CassandraProperties aasCassandraProperties) {

    return new CassandraConnectionProvider(
            aasCassandraProperties.getHosts(),
            aasCassandraProperties.getPort(),
            aasCassandraProperties.getKeyspace(),
            aasCassandraProperties.getUser(),
            aasCassandraProperties.getPassword());
  }

  @Bean("uisCassandraProvider")
  CassandraConnectionProvider uisCassandraProvider(CassandraProperties uisCassandraProperties) {

    return new CassandraConnectionProvider(
            uisCassandraProperties.getHosts(),
            uisCassandraProperties.getPort(),
            uisCassandraProperties.getKeyspace(),
            uisCassandraProperties.getUser(),
            uisCassandraProperties.getPassword());
  }

  @Bean
  BucketsHandler bucketsHandler(CassandraConnectionProvider uisCassandraProvider) {
    return new BucketsHandler(uisCassandraProvider.getSession());
  }

  @Bean
  MethodValidationPostProcessor methodValidationPostProcessor() {
    return new MethodValidationPostProcessor();
  }

  @Bean
  MethodInvokingFactoryBean methodInvokingFactoryBean() {
    MethodInvokingFactoryBean result = new MethodInvokingFactoryBean();
    result.setTargetClass(SecurityContextHolder.class);
    result.setTargetMethod("setStrategyName");
    result.setArguments("MODE_INHERITABLETHREADLOCAL");
    return result;
  }

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(new LoggingFilter());
  }

  @Bean
  RetryAspect retryAspect() {
    return new RetryAspect();
  }

  @Bean
  public CassandraHealthIndicator cassandraHealthIndicator(CassandraConnectionProvider uisCassandraProvider) {
    return new CassandraHealthIndicator(uisCassandraProvider);
  }

  @Bean
  public MeterRegistryCustomizer<MeterRegistry> commonTags() {
    return registry -> registry.config().commonTags(
            "namespace", System.getenv("Namespace"),
            "instance", System.getenv("AppId")
    );
  }

  @Bean
  public CountedAspect countedAspect(MeterRegistry registry) {
    return new CountedAspect(registry);
  }

  @Bean
  public TimedAspect timedAspect(MeterRegistry registry) {
    return new TimedAspect(registry);
  }
}
