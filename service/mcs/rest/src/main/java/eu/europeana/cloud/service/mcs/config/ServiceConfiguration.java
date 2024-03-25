package eu.europeana.cloud.service.mcs.config;

import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.service.commons.utils.RetryAspect;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.persistent.uis.UISClientHandlerImpl;
import eu.europeana.cloud.service.mcs.properties.GeneralProperties;
import eu.europeana.cloud.service.web.common.LoggingFilter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@EnableAsync
@PropertySource(value = "classpath:mcs.properties", ignoreResourceNotFound = true)
@ComponentScan("eu.europeana.cloud.service.mcs.controller")
public class ServiceConfiguration implements WebMvcConfigurer {

  @Bean
  @ConfigurationProperties(prefix = "general")
  GeneralProperties generalProperties() {
    return new GeneralProperties();
  }

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(new LoggingFilter());
  }

  @Override
  public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
    configurer.setTaskExecutor(asyncExecutor());
  }

  @Bean
  Integer objectStoreSizeThreshold() {
    return 524288;
  }


  @Bean
  UISClientHandler uisHandler() {
    return new UISClientHandlerImpl(uisClient());
  }

  @Bean
  UISClient uisClient() {
    return new UISClient(generalProperties().getUisLocation());
  }

  @Bean
  RetryAspect retryAspect() {
    return new RetryAspect();
  }

  @Bean
  AsyncTaskExecutor asyncExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(10);
    executor.setMaxPoolSize(40);
    executor.setQueueCapacity(20);
    executor.setThreadNamePrefix("MCSThreadPool-");
    return executor;
  }

}
