package eu.europeana.cloud.service.aas.rest;

import eu.europeana.cloud.service.aas.authentication.AuthenticationService;
import eu.europeana.cloud.service.aas.config.UnifiedExceptionsMapper;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

//@EnableWebMvc
//@Import({UnifiedExceptionsMapper.class})
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
