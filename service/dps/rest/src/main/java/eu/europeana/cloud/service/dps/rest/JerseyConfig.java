package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.service.commons.logging.LoggingFilter;
import eu.europeana.cloud.service.dps.rest.exceptionmappers.*;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spring.scope.RequestContextFilter;

/**
 * Jersey Configuration for Exception Mappers and Resources
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

        // exception mappers
        register(RuntimeExceptionMapper.class);
        register(TopologyAlreadyExistsExceptionMapper.class);
        register(AccessDeniedOrTopologyDoesNotExistExceptionExceptionMapper.class);
        register(TaskSubmissionExceptionMapper.class);
        register(DpsTaskValidationExceptionMapper.class);
        register(AccessDeniedOrObjectDoesNotExistExceptionMapper.class);

        // resources
        register(TopologyTasksResource.class);
        register(TopologiesResource.class);

    }
}
