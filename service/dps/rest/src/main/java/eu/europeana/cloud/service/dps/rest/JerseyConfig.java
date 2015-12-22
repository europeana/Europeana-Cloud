package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.service.commons.logging.LoggingFilter;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.rest.exceptionmappers.AccessDeniedOrTopologyDoesNotExistExceptionExceptionMapper;
import eu.europeana.cloud.service.dps.rest.exceptionmappers.TaskSubmissionExceptionMapper;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spring.scope.RequestContextFilter;

import eu.europeana.cloud.service.dps.rest.exceptionmappers.RuntimeExceptionMapper;
import eu.europeana.cloud.service.dps.rest.exceptionmappers.TopologyAlreadyExistsExceptionMapper;

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

        // exception mappers
        register(RuntimeExceptionMapper.class);
        register(TopologyAlreadyExistsExceptionMapper.class);
        register(AccessDeniedOrTopologyDoesNotExistExceptionExceptionMapper.class);
        register(TaskSubmissionExceptionMapper.class);

        // resources
        register(TopologyTasksResource.class);
        register(TopologiesResource.class);
    }
}
