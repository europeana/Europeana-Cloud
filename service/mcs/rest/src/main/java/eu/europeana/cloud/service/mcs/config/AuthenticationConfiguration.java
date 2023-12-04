package eu.europeana.cloud.service.mcs.config;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.aas.authentication.CassandraAuthenticationService;
import eu.europeana.cloud.service.aas.authentication.handlers.CloudAuthenticationEntryPoint;
import eu.europeana.cloud.service.aas.authentication.repository.CassandraUserDAO;
import eu.europeana.cloud.service.commons.utils.PasswordEncoderFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.event.LoggerListener;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;

@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true, securedEnabled = true, proxyTargetClass = true)
public class AuthenticationConfiguration extends WebSecurityConfigurerAdapter {

  /*
   * We disable CSRF because our application is consumed only via direct calls to rest api.
   * Additionally, each call that require some sort of authentication or authorization always require to send credentials inside request body,
   * because we don't support remembering password via cookies etc.
   */
  @SuppressWarnings("java:S4502")
  @Override
  protected void configure(HttpSecurity http) throws Exception {
    http.httpBasic()
        .authenticationEntryPoint(cloudAuthenticationEntryPoint())
        .and()
        .sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS)
        .and()
        .csrf().disable();
  }

  @Override
  protected void configure(AuthenticationManagerBuilder auth) throws Exception {
    auth.userDetailsService(authenticationService())
        .passwordEncoder(PasswordEncoderFactory.getPasswordEncoder());
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
  CassandraUserDAO userDAO(CassandraConnectionProvider aasCassandraProvider) {
    return new CassandraUserDAO(aasCassandraProvider);
  }

  @Bean
  CassandraAuthenticationService authenticationService() {
    return new CassandraAuthenticationService();
  }

}
