package eu.europeana.cloud.service.aas.authentication;

import eu.europeana.cloud.common.model.IdentifierErrorInfo;
import eu.europeana.cloud.common.model.User;
import eu.europeana.cloud.service.aas.authentication.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.aas.authentication.exception.UserDoesNotExistException;
import eu.europeana.cloud.service.aas.authentication.exception.UserExistsException;
import eu.europeana.cloud.service.aas.authentication.repository.CassandraUserDAO;
import eu.europeana.cloud.service.aas.authentication.status.IdentifierErrorTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

/**
 * Used throughout the Spring Security framework to pass user specific data.
 *
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 */
public class CassandraAuthenticationService implements UserDetailsService, AuthenticationService {

    @Autowired
    private CassandraUserDAO userDao;

    public CassandraAuthenticationService() {
        // nothing todo
    }
    
    public CassandraAuthenticationService(CassandraUserDAO userDao) {
        this.userDao = userDao;
    }

    @Override
    public UserDetails loadUserByUsername(final String userName)
            throws UsernameNotFoundException {
        try {
            SpringUser user = userDao.getUser(userName);
            if (user == null)
                throw new UsernameNotFoundException("Username '" + userName + "' could not be retrieved from the database!");
            return userDao.getUser(userName);
        } catch (DatabaseConnectionException ex) {
            throw new UsernameNotFoundException("Username '" + userName + "' could not be retrieved due to database error!", ex);
        }
    }

    @Override
    public User getUser(String userName) throws DatabaseConnectionException, UserDoesNotExistException {
        SpringUser user = userDao.getUser(userName);
        if (user == null) {
            throw new UserDoesNotExistException(
                    new IdentifierErrorInfo(
                            IdentifierErrorTemplate.USER_DOES_NOT_EXIST
                                    .getHttpCode(),
                            IdentifierErrorTemplate.USER_DOES_NOT_EXIST
                                    .getErrorInfo(userName)));
        }
        return userDao.getUser(userName);
    }

    @Override
    public void createUser(final User user) throws DatabaseConnectionException, UserExistsException {
        if (userDao.getUser(user.getUsername()) != null) {
            throw new UserExistsException(new IdentifierErrorInfo(
                    IdentifierErrorTemplate.USER_EXISTS.getHttpCode(),
                    IdentifierErrorTemplate.USER_EXISTS.getErrorInfo(user
                            .getUsername())));
        } else {
            userDao.createUser(user);
        }
    }

    @Override
    public void updateUser(final User user)
            throws DatabaseConnectionException, UserDoesNotExistException {
        if (userDao.getUser(user.getUsername()) == null) {
            throw new UserDoesNotExistException(
                    new IdentifierErrorInfo(
                            IdentifierErrorTemplate.USER_DOES_NOT_EXIST
                                    .getHttpCode(),
                            IdentifierErrorTemplate.USER_DOES_NOT_EXIST
                                    .getErrorInfo(user.getUsername())));
        } else {
            userDao.updateUser(user);
        }
    }

}
