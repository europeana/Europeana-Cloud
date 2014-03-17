package eu.europeana.cloud.service.dls.rest;

import org.glassfish.jersey.filter.LoggingFilter;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spring.scope.RequestContextFilter;

/**
 * Jersey Configuration for Exception Mappers and Resources
 * 
 */
public class JerseyConfig extends ResourceConfig {

    /**
     * Register JAX-RS application components.
     */
    public JerseyConfig() {
        super();
        //features
        register(MultiPartFeature.class);

        // filters
        register(RequestContextFilter.class);
        register(LoggingFilter.class);

        // resources
        register(RepresentationSearchResource.class);
    }
}
