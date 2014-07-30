package eu.europeana.cloud.service.aas.authentication;

import java.util.Collection;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import eu.europeana.cloud.service.aas.authentication.User;

/**
 * Provides core information for every user interacting with the ecloud.
 * Implementation compatible with spring security.
 * 
 * (username, password..)
 * 
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 *
 */
public class SpringSecurityUserImpl implements User, UserDetails {
	
	private final String username;
	
	private final String password;

	public SpringSecurityUserImpl(final String username, final String password) {
		
		this.username = username;
		this.password = password;
	}

	@Override
	public Collection<? extends GrantedAuthority> getAuthorities() {
		// TODO 
		return null;
	}

	@Override
	public String getPassword() {
		return password;
	}

	@Override
	public String getUsername() {
		return username;
	}

	@Override
	public boolean isAccountNonExpired() {
		return true;
	}

	@Override
	public boolean isAccountNonLocked() {
		return true;
	}

	@Override
	public boolean isCredentialsNonExpired() {
		return true;
	}

	@Override
	public boolean isEnabled() {
		return true;
	}
}
