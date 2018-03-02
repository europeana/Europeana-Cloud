package eu.europeana.cloud.service.aas.rest;

import eu.europeana.cloud.common.model.User;
import eu.europeana.cloud.common.web.AASParamConstants;
import eu.europeana.cloud.service.aas.authentication.AuthenticationService;
import eu.europeana.cloud.service.aas.authentication.exception.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

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
            @QueryParam(AASParamConstants.P_USER_NAME) String username,
            @QueryParam(AASParamConstants.P_PASSWORD) String password)
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
            @QueryParam(AASParamConstants.P_USER_NAME) String username)
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
            @QueryParam(AASParamConstants.P_USER_NAME) String username,
            @QueryParam(AASParamConstants.P_PASSWORD) String password)
            throws DatabaseConnectionException, UserDoesNotExistException,
            InvalidPasswordException {

        authenticationService.updateUser(new User(username, password));
        return Response.ok("Cloud user updated.").build();
    }
}
