package eu.europeana.cloud.service.commons.utils;

import static org.mockito.Mockito.spy;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.test.context.ContextConfiguration;

@ContextConfiguration
@EnableAspectJAutoProxy
public class RetryAspectConfiguration {

  @Bean
  public RetryAspect retryAspect() {
    return new RetryAspect();
  }

  @Bean
  public AspectedTestSpringCtx aspectedTest() {
    return spy(AspectedTestSpringCtxImpl.class);
  }
}
