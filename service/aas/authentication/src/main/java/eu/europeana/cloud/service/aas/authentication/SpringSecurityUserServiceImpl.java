package eu.europeana.cloud.service.aas.authentication;

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
public class SpringSecurityUserServiceImpl implements UserDetailsService {
	
	@Autowired
	UserDAO userDao;

	@Override
	public UserDetails loadUserByUsername(final String userName)
			throws UsernameNotFoundException {
		
		User ecloudUser = userDao.getUser(userName);
		return mapToSpringSecurity(ecloudUser);
	}
	
	/**
	 * @return Maps an Ecloud User to a Spring Security user.
	 *  
	 * @param user an Ecloud User
	 */
	private SpringSecurityUserImpl mapToSpringSecurity(final User user) {
		
		final SpringSecurityUserImpl ssUser = new SpringSecurityUserImpl(user.getUsername(), user.getPassword());
		return ssUser;
	}
}
