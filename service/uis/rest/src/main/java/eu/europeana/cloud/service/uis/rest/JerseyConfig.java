package eu.europeana.cloud.service.uis.rest;

import eu.europeana.cloud.service.uis.exception.*;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.spring.scope.RequestContextFilter;

/**
 * Jersey Configuration for Exception Mappers and Resources
 *
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
public class JerseyConfig extends ResourceConfig {

    /**
     * Creates a new instance of this class.
     */
    public JerseyConfig() {
        super();
        register(RequestContextFilter.class);
        register(CloudIdAlreadyExistExceptionMapper.class);
        register(eu.europeana.cloud.service.commons.logging.LoggingFilter.class);
        register(CloudIdDoesNotExistExceptionMapper.class);
        register(DatabaseConnectionExceptionMapper.class);
        register(IdHasBeenMappedExceptionMapper.class);
        register(ProviderDoesNotExistExceptionMapper.class);
        register(RecordDatasetEmptyExceptionMapper.class);
        register(RecordDoesNotExistExceptionMapper.class);
        register(RecordExistsExceptionMapper.class);
        register(RecordIdDoesNotExistExceptionMapper.class);
        register(ProviderAlreadyExistsExceptionMapper.class);
        register(WebApplicationsExceptionMapper.class);
        register(RuntimeExceptionMapper.class);
        register(UniqueIdentifierResource.class);
        register(DataProviderResource.class);
        register(DataProvidersResource.class);
        register(DataProviderActivationResource.class);
    }
}
