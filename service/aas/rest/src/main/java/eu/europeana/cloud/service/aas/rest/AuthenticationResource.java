package eu.europeana.cloud.service.aas.rest;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.qmino.miredot.annotations.ReturnType;

import eu.europeana.cloud.common.model.User;
import eu.europeana.cloud.common.web.AASParamConstants;
import eu.europeana.cloud.service.aas.authentication.AuthenticationService;
import eu.europeana.cloud.service.aas.authentication.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.aas.authentication.exception.InvalidPasswordException;
import eu.europeana.cloud.service.aas.authentication.exception.InvalidUsernameException;
import eu.europeana.cloud.service.aas.authentication.exception.UserExistsException;

/**
 * Implementation of the Unique Identifier Service.
 *
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 17, 2013
 */
@Component
@Path("/user")
@Scope("request")
public class AuthenticationResource {

    @Autowired
    private AuthenticationService authenticationService;

    private static final String USER_NAME = "username";

    @PathParam(USER_NAME)
    private String username;

    /**
     * Invoke the generation of a new user
     *
     * @param username
     * @param password
     * @return The newly created CloudId
     * @throws DatabaseConnectionException
     * @throws UserExistsException
     * @throws InvalidUsernameException
     * @throws InvalidPasswordException
     */
    @POST
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @ReturnType("eu.europeana.cloud.common.model.CloudId")
//    @PreAuthorize("isAuthenticated()")
    public Response createCloudUser(@QueryParam(AASParamConstants.Q_USER_NAME) String username,
            @QueryParam(AASParamConstants.Q_PASSWORD) String password)
            throws
            DatabaseConnectionException, UserExistsException,
            InvalidUsernameException, InvalidPasswordException {
        authenticationService.createUser(new User(username, password));
        return Response.ok("Cloud user was created!").build();
    }

//    /**
//     * Invoke the generation of a cloud identifier using the provider identifier
//     * 
//     * @param providerId
//     * @param recordId
//     * @return The newly created CloudId
//     * @throws DatabaseConnectionException
//     * @throws RecordDoesNotExistException
//     * @throws ProviderDoesNotExistException
//     * @throws RecordDatasetEmptyException
//     */
//    @GET
////    @Path("cloudIds")
//    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
//    @ReturnType("eu.europeana.cloud.common.model.CloudId")
//    public Response getCloudId(@QueryParam(UISParamConstants.Q_PROVIDER) String providerId,
//            @QueryParam(UISParamConstants.Q_RECORD_ID) String recordId)
//            throws DatabaseConnectionException, RecordDoesNotExistException,
//            ProviderDoesNotExistException, RecordDatasetEmptyException {
//        return Response.ok(uniqueIdentifierService.getCloudId(providerId, recordId)).build();
//    }
//
//    /**
//     * Retrieve a list of record Identifiers associated with a cloud identifier
//     * 
//     * @return A list of record identifiers
//     * @throws DatabaseConnectionException
//     * @throws CloudIdDoesNotExistException
//     * @throws ProviderDoesNotExistException
//     * @throws RecordDatasetEmptyException
//     */
//    @GET
////    @Path("cloudIds/{" + CLOUDID + "}")
//    @Path("{" + CLOUDID + "}")
//    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
//    @ReturnType("eu.europeana.cloud.common.response.ResultSlice")
//    public Response getLocalIds() throws DatabaseConnectionException, CloudIdDoesNotExistException,
//            ProviderDoesNotExistException, RecordDatasetEmptyException {
//        ResultSlice<CloudId> pList = new ResultSlice<>();
//        pList.setResults(uniqueIdentifierService.getLocalIdsByCloudId(cloudId));
//        return Response.ok(pList).build();
//    }
//
//    /**
//     * Remove a cloud identifier and all the associations to its record identifiers
//     * 
//     * @return Confirmation that the selected cloud identifier is removed
//     * @throws DatabaseConnectionException
//     * @throws CloudIdDoesNotExistException
//     * @throws ProviderDoesNotExistException
//     * @throws RecordIdDoesNotExistException
//     */
//    @DELETE
////    @Path("cloudIds/{" + CLOUDID + "}")
//    @Path("{" + CLOUDID + "}")
//    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
//    @PreAuthorize("hasPermission(#cloudId, 'eu.europeana.cloud.common.model.CloudId', delete)")
//    public Response deleteCloudId() throws DatabaseConnectionException,
//            CloudIdDoesNotExistException, ProviderDoesNotExistException,
//            RecordIdDoesNotExistException {
//
//        uniqueIdentifierService.deleteCloudId(cloudId);
//        return Response.ok("CloudId marked as deleted").build();
//    }
//
//    /**
//     * @return Name of the currently logged in user
//     */
//    private String getUsername() {
//        String username = null;
//        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
//        if (auth != null) {
//            if (auth.getPrincipal() instanceof UserDetails) {
//                username = ((UserDetails)auth.getPrincipal()).getUsername();
//            } else {
//                username = auth.getPrincipal().toString();
//            }
//        }
//        return username;
//    }
}
