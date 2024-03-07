package eu.europeana.cloud.service.mcs.utils.testcontexts;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.config.annotation.ObjectPostProcessor;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;

@TestConfiguration
public class TestAuthentificationConfiguration {


  @Bean
  protected AuthenticationManager authenticationManager(ObjectPostProcessor objectPostProcessor) throws Exception {
    AuthenticationManagerBuilder auth = new AuthenticationManagerBuilder(objectPostProcessor);
    auth.inMemoryAuthentication().withUser("admin").password("admin").roles("ADMIN", "USER")
        .and().withUser("Robin_Van_Persie").password("Feyenoord").roles("USER")
        .and().withUser("Cristiano").password("Ronaldo").roles("USER")
        .and().withUser("Anonymous").password("Anonymous").roles("ANONYMOUS")
        .and().passwordEncoder(NoOpPasswordEncoder.getInstance());

    return auth.build();
  }
}
