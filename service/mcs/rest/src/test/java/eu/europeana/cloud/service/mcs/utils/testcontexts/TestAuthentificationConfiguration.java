package eu.europeana.cloud.service.mcs.utils.testcontexts;

import eu.europeana.cloud.service.mcs.config.AuthenticationConfiguration;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;

@TestConfiguration
public class TestAuthentificationConfiguration extends AuthenticationConfiguration {


  @Bean
  protected AuthenticationManager configure(AuthenticationManagerBuilder auth) throws Exception {
    auth.inMemoryAuthentication()
        .passwordEncoder(NoOpPasswordEncoder.getInstance())
        .withUser("admin").password("admin").roles("ADMIN", "USER")
        .and().withUser("Robin_Van_Persie").password("Feyenoord").roles("USER")
        .and().withUser("Cristiano").password("Ronaldo").roles("USER")
        .and().withUser("Anonymous").password("Anonymous").roles("ANONYMOUS");
    return auth.build();
  }
}
