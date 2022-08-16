package eu.europeana.cloud.service.aas.rest;

import eu.europeana.cloud.service.aas.authentication.AuthenticationService;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.security.crypto.password.PasswordEncoder;

public class TestConfiguration {

    @Bean
    public AuthenticationService authenticationService() {
        return Mockito.mock(AuthenticationService.class);
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return Mockito.mock(PasswordEncoder.class);
    }

    @Bean
    public AuthenticationResource authenticationResource(AuthenticationService authenticationService, PasswordEncoder passwordEncoder){
        return new AuthenticationResource(authenticationService, passwordEncoder);
    }
}
