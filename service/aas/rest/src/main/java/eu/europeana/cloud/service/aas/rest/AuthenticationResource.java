package eu.europeana.cloud.service.aas.rest;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.User;
import eu.europeana.cloud.common.web.AASParamConstants;
import eu.europeana.cloud.service.aas.authentication.AuthenticationService;
import eu.europeana.cloud.service.aas.authentication.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.aas.authentication.exception.InvalidPasswordException;
import eu.europeana.cloud.service.aas.authentication.exception.InvalidUsernameException;
import eu.europeana.cloud.service.aas.authentication.exception.UserExistsException;

/**
 * AAS: just a single call that creates an ecloud user
 *
 */
@Component
@Path("/create-user")
@Scope("request")
public class AuthenticationResource {

    @Autowired
    private AuthenticationService authenticationService;

    /**
     * Creates a new ecloud-user with the specified username + password.
     */
    @POST
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Response createCloudUser(@QueryParam(AASParamConstants.Q_USER_NAME) String username,
    		@QueryParam(AASParamConstants.Q_PASSWORD) String password)
    				throws DatabaseConnectionException, UserExistsException,
    					InvalidUsernameException, InvalidPasswordException {
    	
        authenticationService.createUser(new User(username, password));
        return Response.ok("Cloud user was created!").build();
    }
}
