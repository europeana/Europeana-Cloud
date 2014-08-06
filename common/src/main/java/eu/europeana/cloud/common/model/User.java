package eu.europeana.cloud.common.model;

import java.util.Objects;

/**
 * This class represents a user in the cloud environment.
 *
 * @author Markus Muhr (markus.muhr@theeuropeanlibrary.org)
 * @since 06.08.2014
 */
public class User {

    private String username;

    private String password;

    public User() {
        this.username = null;
        this.password = null;
    }

    public User(final String username, final String password) {
        this.username = username;
        this.password = password;
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

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 89 * hash + Objects.hashCode(this.username);
        hash = 89 * hash + Objects.hashCode(this.password);
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
        if (!Objects.equals(this.username, other.username)) {
            return false;
        }
        if (!Objects.equals(this.password, other.password)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "CloudUser{" + "username=" + username + ", password=" + password + '}';
    }
}
