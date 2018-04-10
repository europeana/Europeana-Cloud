package eu.europeana.cloud.service.aas.authentication;

import eu.europeana.cloud.common.model.User;
import eu.europeana.cloud.service.aas.authentication.exception.*;

/**
 * Specifies functionality for authentication.
 *
 * @author Markus Muhr (markus.muhr@theeuropeanlibrary.org)
 */
public interface AuthenticationService {

    void createUser(User user) throws DatabaseConnectionException, UserExistsException,
            InvalidUsernameException, InvalidPasswordException;

    void updateUser(User user)
            throws DatabaseConnectionException, UserDoesNotExistException,
            InvalidPasswordException;

    void deleteUser(String userName) throws DatabaseConnectionException, UserDoesNotExistException;

    User getUser(String userName) throws DatabaseConnectionException, UserDoesNotExistException;
}
