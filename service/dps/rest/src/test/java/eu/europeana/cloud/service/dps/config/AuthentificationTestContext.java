package eu.europeana.cloud.service.dps.config;

import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.ObjectPostProcessor;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;

@Configuration
@Import({eu.europeana.cloud.service.aas.authentication.handlers.CloudAuthenticationEntryPoint.class,
    eu.europeana.cloud.service.aas.authentication.handlers.CloudAuthenticationSuccessHandler.class,
    org.springframework.security.web.authentication.SimpleUrlAuthenticationFailureHandler.class,
    org.springframework.security.authentication.event.LoggerListener.class})
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true, securedEnabled = true, proxyTargetClass = true)
public class AuthentificationTestContext extends WebSecurityConfigurerAdapter {

  /*
   * We disable CSRF because our application is consumed only via direct calls to rest api.
   * Additionally, each call that require some sort of authentication or authorization always require to send credentials inside request body,
   * because we don't support remembering password via cookies etc.
   */
  @SuppressWarnings("java:S4502")
  protected void configure(HttpSecurity http) throws Exception {
    http.
        httpBasic().and().
        sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS).and().
        csrf().disable();
  }

  @Autowired
  @Bean
  protected AuthenticationManager authenticationManager(ObjectPostProcessor objectPostProcessor) throws Exception {
    AuthenticationManagerBuilder auth = new AuthenticationManagerBuilder(objectPostProcessor);
    auth.inMemoryAuthentication().withUser("admin").password("admin").roles("ADMIN", "USER")
        .and().withUser("Robin_Van_Persie").password("Feyenoord").roles("USER")
        .and().withUser("Cristiano").password("Ronaldo").roles("USER")
        .and().withUser("Anonymous").password("Anonymous").roles("ANONYMOUS")
        .and().passwordEncoder(NoOpPasswordEncoder.getInstance());
    ;

    return auth.build();
  }

  @Bean
  public RecordExecutionSubmitService recordExecutionSubmitService() {
    return Mockito.mock(RecordExecutionSubmitService.class);
  }

  @Bean
  public ProcessedRecordsDAO processedRecordsDAO() {
    return Mockito.mock(ProcessedRecordsDAO.class);
  }

  @Bean
  public TaskStatusChecker taskStatusChecker() {
    return Mockito.mock(TaskStatusChecker.class);
  }

  @Bean
  public KafkaTopicSelector kafkaTopicSelector() {
    return Mockito.mock(KafkaTopicSelector.class);
  }

}
