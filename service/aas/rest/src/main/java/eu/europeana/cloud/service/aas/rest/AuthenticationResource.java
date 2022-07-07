package eu.europeana.cloud.service.aas.rest;

import eu.europeana.cloud.common.model.User;
import eu.europeana.cloud.common.web.AASParamConstants;
import eu.europeana.cloud.service.aas.authentication.AuthenticationService;
import eu.europeana.cloud.service.aas.authentication.exception.*;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;

/**
 * AAS: just a single call that creates an ecloud user
 */
@RestController
@RequestMapping("/")
public class AuthenticationResource {

    //@Autowired
    private final AuthenticationService authenticationService;

    //@Autowired
    private final PasswordEncoder passwordEncoder;

    public AuthenticationResource(AuthenticationService authenticationService, PasswordEncoder passwordEncoder) {
        this.authenticationService = authenticationService;
        this.passwordEncoder = passwordEncoder;
    }

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

        authenticationService.createUser(new User(username, passwordEncoder.encode(password)));
        return ResponseEntity.ok("Cloud user was created!");
    }

    /**
     * Deletes a user with the specified username.
     */
    @PostMapping(value = "/delete-user", produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public ResponseEntity<String> deleteCloudUser(
            @RequestParam(AASParamConstants.P_USER_NAME) String username)
            throws DatabaseConnectionException, UserDoesNotExistException {

        authenticationService.deleteUser(username);
        return ResponseEntity.ok("Cloud user is gone. Bye bye.");
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
