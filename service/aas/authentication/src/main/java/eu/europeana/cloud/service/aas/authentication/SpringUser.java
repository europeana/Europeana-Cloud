package eu.europeana.cloud.service.aas.authentication;

import eu.europeana.cloud.common.model.User;
import java.util.Collection;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

/**
 * Provides core information for every user interacting with the ecloud.
 * Implementation compatible with spring security.
 *
 * (username, password..)
 *
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 */
public class SpringUser extends User implements UserDetails {

    public SpringUser(final String username, final String password) {
        super(username, password);
    }

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        // TODO 
        return null;
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
