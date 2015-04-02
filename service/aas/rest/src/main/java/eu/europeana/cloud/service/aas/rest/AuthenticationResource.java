package eu.europeana.cloud.service.aas.rest;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.User;
import eu.europeana.cloud.common.web.AASParamConstants;
import eu.europeana.cloud.service.aas.authentication.AuthenticationService;
import eu.europeana.cloud.service.aas.authentication.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.aas.authentication.exception.InvalidPasswordException;
import eu.europeana.cloud.service.aas.authentication.exception.InvalidUsernameException;
import eu.europeana.cloud.service.aas.authentication.exception.UserDoesNotExistException;
import eu.europeana.cloud.service.aas.authentication.exception.UserExistsException;

/**
 * AAS: just a single call that creates an ecloud user
 */
@Component
@Path("/")
@Scope("request")
public class AuthenticationResource {

    @Autowired
    private AuthenticationService authenticationService;

    /**
     * Creates a new ecloud-user with the specified username + password.
     */
    @POST
    @Path("/create-user")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public Response createCloudUser(
            @QueryParam(AASParamConstants.Q_USER_NAME) String username,
            @QueryParam(AASParamConstants.Q_PASSWORD) String password)
            throws DatabaseConnectionException, UserExistsException,
            InvalidUsernameException, InvalidPasswordException {

        authenticationService.createUser(new User(username, password));
        return Response.ok("Cloud user was created!").build();
    }

    /**
     * Deletes a user with the specified username.
     */
    @POST
    @Path("/delete-user")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public Response deleteCloudUser(
            @QueryParam(AASParamConstants.Q_USER_NAME) String username)
            throws DatabaseConnectionException, UserDoesNotExistException {

        authenticationService.deleteUser(username);
        return Response.ok("Cloud user is gone. Bye bye.").build();
    }

    /**
     * Updates an ecloud-user with the specified username + password.
     */
    @POST
    @Path("/update-user")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public Response updateCloudUser(
            @QueryParam(AASParamConstants.Q_USER_NAME) String username,
            @QueryParam(AASParamConstants.Q_PASSWORD) String password)
            throws DatabaseConnectionException, UserDoesNotExistException,
            InvalidPasswordException {

        authenticationService.updateUser(new User(username, password));
        return Response.ok("Cloud user updated.").build();
    }
}
