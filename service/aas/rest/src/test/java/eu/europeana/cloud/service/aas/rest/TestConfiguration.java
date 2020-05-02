package eu.europeana.cloud.service.aas.rest;

import eu.europeana.cloud.service.aas.authentication.AuthenticationService;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;

public class TestConfiguration {

    @Bean
    public AuthenticationService authenticationService() {
        return Mockito.mock(AuthenticationService.class);
    }

    @Bean
    public AuthenticationResource authenticationResource(){
        return new AuthenticationResource();
    }
}
