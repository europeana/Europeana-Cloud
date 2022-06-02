package eu.europeana.cloud.service.aas.rest;

import eu.europeana.cloud.common.model.User;
import eu.europeana.cloud.common.web.AASParamConstants;
import eu.europeana.cloud.service.aas.authentication.AuthenticationService;
import eu.europeana.cloud.service.aas.authentication.exception.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

/**
 * AAS: just a single call that creates an ecloud user
 */
@RestController
@RequestMapping("/")
public class AuthenticationResource {

    @Autowired
    private AuthenticationService authenticationService;

    /**
     * Creates a new ecloud-user with the specified username + password.
     */
    @PostMapping(value = "/create-user", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public ResponseEntity<String> createCloudUser(
            @RequestParam(AASParamConstants.P_USER_NAME) String username,
            @RequestParam(AASParamConstants.P_PASS_TOKEN) String password)
            throws DatabaseConnectionException, UserExistsException,
            InvalidUsernameException, InvalidPasswordException {

        authenticationService.createUser(new User(username, password));
        return ResponseEntity.ok("Cloud user was created!");
    }

    @PostMapping(value = "/user/{username}/locked", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public ResponseEntity<String> lockUser(
            @PathVariable(AASParamConstants.P_USER_NAME) String username)
            throws DatabaseConnectionException, UserDoesNotExistException {
        authenticationService.lockUser(username);
        return ResponseEntity.ok("Cloud user is locked.");
    }

    @DeleteMapping(value = "/user/{username}/locked", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public ResponseEntity<String> unlockUser(
            @PathVariable(AASParamConstants.P_USER_NAME) String username)
            throws DatabaseConnectionException, UserDoesNotExistException {
        authenticationService.unlockUser(username);
        return ResponseEntity.ok("Cloud user is unlocked.");
    }


    /**
     * Updates an ecloud-user with the specified username + password.
     */
    @PostMapping(value = "/update-user", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public ResponseEntity<String> updateCloudUser(
            @RequestParam(AASParamConstants.P_USER_NAME) String username,
            @RequestParam(AASParamConstants.P_PASS_TOKEN) String password)
            throws DatabaseConnectionException, UserDoesNotExistException,
            InvalidPasswordException {

        authenticationService.updateUser(new User(username, password));
        return ResponseEntity.ok("Cloud user updated.");
    }
}
