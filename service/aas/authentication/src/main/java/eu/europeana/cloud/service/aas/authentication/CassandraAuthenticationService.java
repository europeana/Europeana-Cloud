package eu.europeana.cloud.service.aas.authentication;

import eu.europeana.cloud.common.model.User;
import eu.europeana.cloud.service.aas.authentication.exception.*;
import eu.europeana.cloud.service.aas.authentication.repository.CassandraUserDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.core.userdetails.UsernameNotFoundException;

/**
 *
 * Used throughout the Spring Security framework to pass user specific data.
 *
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 *
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
            return userDao.getUser(userName);
        } catch (DatabaseConnectionException ex) {
            throw new UsernameNotFoundException("Username '" + userName + "' could not be retrieved due to database error!", ex);
        } catch (UserDoesNotExistException ex) {
            throw new UsernameNotFoundException("Username '" + userName + "' could not be retrieved from the database!", ex);
        }
    }

    @Override
    public User getUser(String userName) throws DatabaseConnectionException, UserDoesNotExistException {
        return userDao.getUser(userName);
    }

    @Override
    public void createUser(final User user) throws DatabaseConnectionException, UserExistsException,
            InvalidUsernameException, InvalidPasswordException {
        userDao.createUser(user);
    }

    @Override
    public void updateUser(final User user)
            throws DatabaseConnectionException, UserDoesNotExistException,
            InvalidPasswordException {
        userDao.updateUser(user);
    }

    @Override
    public void deleteUser(final String userName) throws DatabaseConnectionException, UserDoesNotExistException {
        userDao.deleteUser(userName);
    }

}
