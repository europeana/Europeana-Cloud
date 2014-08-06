package eu.europeana.cloud.service.aas.authentication;

import eu.europeana.cloud.service.aas.authentication.repository.CassandraUserDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.userdetails.User;
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
public class CloudUserDetailsService implements UserDetailsService {

    @Autowired
    CassandraUserDAO userDao;

    @Override
    public UserDetails loadUserByUsername(final String userName)
            throws UsernameNotFoundException {
//        UserDetails ecloudUser = userDao.getUser(userName);
//        return mapToSpringSecurity(ecloudUser);
        return userDao.getUser(userName);
    }

    /**
     * @return Maps an Ecloud User to a Spring Security user.
     *
     * @param user an Ecloud User
     */
    private CloudUserDetails mapToSpringSecurity(final User user) {
        final CloudUserDetails ssUser = new CloudUserDetails(user.getUsername(), user.getPassword());
        return ssUser;
    }
}
