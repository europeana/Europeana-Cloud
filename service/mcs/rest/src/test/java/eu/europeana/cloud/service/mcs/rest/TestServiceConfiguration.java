package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.service.mcs.config.ServiceConfiguration;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;

import java.util.Arrays;

@Configuration
public class TestServiceConfiguration extends ServiceConfiguration {

    public TestServiceConfiguration(Environment environment) {
        super(environment);
    }

    @Override
    public void configureContentNegotiation(ContentNegotiationConfigurer configurer) {
        configurer.defaultContentType(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN);
        configurer.defaultContentTypeStrategy(nativeWebRequest ->
                Arrays.asList(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML,  MediaType.TEXT_PLAIN));
    }
}
