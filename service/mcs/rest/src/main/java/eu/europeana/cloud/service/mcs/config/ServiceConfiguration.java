package eu.europeana.cloud.service.mcs.config;

import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.service.commons.utils.RetryAspect;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.persistent.uis.UISClientHandlerImpl;
import eu.europeana.cloud.service.mcs.properties.GeneralProperties;
import eu.europeana.cloud.service.web.common.LoggingFilter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.web.servlet.DispatcherServletAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.DispatcherServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.multipart.commons.CommonsMultipartResolver;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@EnableAsync
@PropertySource("classpath:mcs.properties")
public class ServiceConfiguration implements WebMvcConfigurer {

  private final GeneralProperties generalProperties;

  @Autowired
  public ServiceConfiguration(GeneralProperties generalProperties) {
    this.generalProperties = generalProperties;
  }

  private static final long MAX_UPLOAD_SIZE = (long) 128 * 1024 * 1024; //128MB

  @Override
  public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(new LoggingFilter());
  }

  @Override
  public void configureAsyncSupport(AsyncSupportConfigurer configurer) {
    configurer.setTaskExecutor(asyncExecutor());
  }

  @Bean
  public DispatcherServlet dispatcherServlet() {
    return new DispatcherServlet();
  }

  @Bean
  public DispatcherServletRegistrationBean dispatcherServletRegistration() {
    DispatcherServletRegistrationBean registration = new DispatcherServletRegistrationBean(dispatcherServlet(),
        generalProperties.getContextPath());
    registration.setName(DispatcherServletAutoConfiguration.DEFAULT_DISPATCHER_SERVLET_REGISTRATION_BEAN_NAME);
    return registration;
  }


  @Bean
  public Integer objectStoreSizeThreshold() {
    return 524288;
  }


  @Bean
  public UISClientHandler uisHandler() {
    return new UISClientHandlerImpl();
  }

  @Bean
  public UISClient uisClient() {
    return new UISClient(generalProperties.getUisLocation());
  }



  @SuppressWarnings("S5693") // Limit size is part of system requirements
  @Bean
  public CommonsMultipartResolver multipartResolver() {
    CommonsMultipartResolver multipartResolver = new CommonsMultipartResolver();
    multipartResolver.setMaxUploadSize(MAX_UPLOAD_SIZE);
    return multipartResolver;
  }

  @Bean
  public RetryAspect retryAspect() {
    return new RetryAspect();
  }

  @Bean
  public AsyncTaskExecutor asyncExecutor() {
    ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
    executor.setCorePoolSize(10);
    executor.setMaxPoolSize(40);
    executor.setQueueCapacity(20);
    executor.setThreadNamePrefix("MCSThreadPool-");
    return executor;
  }

}
