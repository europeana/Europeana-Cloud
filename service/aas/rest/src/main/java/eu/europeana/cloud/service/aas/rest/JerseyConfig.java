package eu.europeana.cloud.service.aas.rest;

import eu.europeana.cloud.service.aas.rest.exception.*;
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
        register(eu.europeana.cloud.service.commons.logging.LoggingFilter.class);
        register(DatabaseConnectionExceptionMapper.class);
        register(InvalidPasswordExceptionMapper.class);
        register(InvalidUsernameExceptionMapper.class);
        register(UserExistsExceptionMapper.class);
        register(UserDoesNotExistExceptionMapper.class);
        register(AuthenticationResource.class);
    }
}
