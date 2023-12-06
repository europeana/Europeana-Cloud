package eu.europeana.cloud.common.model;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * This class represents a user in the cloud environment.
 *
 * @author Markus Muhr (markus.muhr@theeuropeanlibrary.org)
 * @since 06.08.2014
 */
public class User {

  private String username;

  private String password;

  private boolean locked = false;

  /**
   * for example: 'ROLE_ADMIN' if the current user is an admin
   */
  private Set<String> roles = new HashSet<>(0);

  public User() {
    this.username = null;
    this.password = null;
  }

  public User(final String username, final String password) {
    this.username = username;
    this.password = password;
  }

  public User(final String username, final String password, final Set<String> roles) {
    this.username = username;
    this.password = password;
    this.roles = roles;
  }

  public String getPassword() {
    return password;
  }

  public String getUsername() {
    return username;
  }

  public void setPassword(final String password) {
    this.password = password;
  }

  public void setUsername(final String username) {
    this.username = username;
  }

  public Set<String> getRoles() {
    return roles;
  }

  public boolean isLocked() {
    return locked;
  }

  public void setLocked(boolean locked) {
    this.locked = locked;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 89 * hash + Objects.hashCode(this.username);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final User other = (User) obj;
    return Objects.equals(this.username, other.username);
  }

  @Override
  public String toString() {
    return "CloudUser{" + "username=" + username + ", password=" + password + '}';
  }
}
