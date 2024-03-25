package eu.europeana.cloud.service.mcs.config;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.aas.authentication.CassandraAuthenticationService;
import eu.europeana.cloud.service.aas.authentication.handlers.CloudAuthenticationEntryPoint;
import eu.europeana.cloud.service.aas.authentication.repository.CassandraUserDAO;
import eu.europeana.cloud.service.commons.utils.PasswordEncoderFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.event.LoggerListener;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Configures Spring authentication in MCS Application
 */
@Configuration
@EnableWebSecurity
@EnableMethodSecurity(securedEnabled = true, proxyTargetClass = true)
public class AuthenticationConfiguration {

  /*
   * We disable CSRF because our application is consumed only via direct calls to rest api.
   * Additionally, each call that require some sort of authentication or authorization always require to send credentials inside request body,
   * because we don't support remembering password via cookies etc.
   */
  @SuppressWarnings("java:S4502")
  @Bean
  protected SecurityFilterChain configure(HttpSecurity http) throws Exception {
    http.httpBasic(configurer ->
            configurer.authenticationEntryPoint(cloudAuthenticationEntryPoint()))
        .sessionManagement(configurer ->
            configurer.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
        .csrf(AbstractHttpConfigurer::disable);

    return http.build();
  }

  @Bean
  PasswordEncoder passwordEncoder(){
    return PasswordEncoderFactory.getPasswordEncoder();
  }

  @Bean
  CloudAuthenticationEntryPoint cloudAuthenticationEntryPoint() {
    return new CloudAuthenticationEntryPoint();
  }

  @Bean
  LoggerListener loggerListener() {
    return new LoggerListener();
  }

  /* ========= AUTHENTICATION STORAGE (USERNAME + PASSWORD TABLES IN CASSANDRA) ========= */

  @Bean
  CassandraUserDAO userDAO(
          @Qualifier("aasCassandraProvider") CassandraConnectionProvider aasCassandraProvider) {
    return new CassandraUserDAO(aasCassandraProvider);
  }

  @Bean
  UserDetailsService authenticationService() {
    return new CassandraAuthenticationService();
  }

}
