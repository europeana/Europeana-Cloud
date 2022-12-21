package eu.europeana.cloud.service.aas.authentication;

import eu.europeana.cloud.common.model.User;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;

/**
 * Provides core information for every user interacting with the ecloud. Implementation compatible with spring security.
 * <p>
 * (username, password..)
 *
 * @author emmanouil.koufakis@theeuropeanlibrary.org
 */
public class SpringUser extends User implements UserDetails {

  private static final long serialVersionUID = 1L;

  private List<GrantedAuthority> roles = new ArrayList<>(0);

  public SpringUser(final String username, final String password, final Set<String> userRoles, boolean locked) {
    super(username, password);
    this.setLocked(locked);
    this.roles = mapToStringRoles(userRoles);
  }

  public SpringUser(final String username, final String password) {
    super(username, password);
  }

  private List<GrantedAuthority> mapToStringRoles(final Set<String> userRoles) {

    Set<GrantedAuthority> setAuths = new HashSet<>();

    // Build authorities
    for (String userRole : userRoles) {
      setAuths.add(new SimpleGrantedAuthority(userRole));
    }

    return new ArrayList<>(setAuths);
  }

  @Override
  public Collection<? extends GrantedAuthority> getAuthorities() {
    return roles;
  }

  @Override
  public boolean isAccountNonExpired() {
    return true;
  }

  @Override
  public boolean isAccountNonLocked() {
    return !isLocked();
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
