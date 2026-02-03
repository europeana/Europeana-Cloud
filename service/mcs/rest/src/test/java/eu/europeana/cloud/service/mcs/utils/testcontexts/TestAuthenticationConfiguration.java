package eu.europeana.cloud.service.mcs.utils.testcontexts;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;

@TestConfiguration
public class TestAuthenticationConfiguration {


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
}
