package eu.europeana.cloud.service.dps.config;

import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;

@Configuration
@Import({eu.europeana.cloud.service.aas.authentication.handlers.CloudAuthenticationEntryPoint.class,
        eu.europeana.cloud.service.aas.authentication.handlers.CloudAuthenticationSuccessHandler.class,
        org.springframework.security.web.authentication.SimpleUrlAuthenticationFailureHandler.class,
        org.springframework.security.authentication.event.LoggerListener.class})
@EnableWebSecurity
@EnableMethodSecurity(prePostEnabled = true, securedEnabled = true, proxyTargetClass = true)
public class AuthentificationTestContext {


  @Bean
  public UserDetailsService userDetailsService() {
    return new InMemoryUserDetailsManager(
            User.withUsername("admin")
                    .password("admin")
                    .roles("ADMIN", "USER")
                    .build(),
            User.withUsername("Robin_Van_Persie")
                    .password("Feyenoord")
                    .roles("USER")
                    .build(),
            User.withUsername("Cristiano")
                    .password("Ronaldo")
                    .roles("USER")
                    .build(),
            User.withUsername("Anonymous")
                    .password("Anonymous")
                    .roles("ANONYMOUS")
                    .build()
    );
  }

  @Bean
  @SuppressWarnings("deprecation")
  public PasswordEncoder passwordEncoder() {
    return NoOpPasswordEncoder.getInstance();
  }

  @Bean("authenticationManager")
  public AuthenticationManager authenticationManager(
          UserDetailsService userDetailsService,
          PasswordEncoder passwordEncoder) {

    DaoAuthenticationProvider provider = new DaoAuthenticationProvider(userDetailsService);
    provider.setPasswordEncoder(passwordEncoder);

    return new ProviderManager(provider);
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
