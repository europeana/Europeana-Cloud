package eu.europeana.cloud.service.aas.authentication;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;

/**
 * Utility class containing function for spring security usage.
 *
 * @author Markus Muhr (markus.muhr@theeuropeanlibrary.org)
 */
public final class SpringUserUtils {

  private SpringUserUtils() {
  }

  /**
   * @return Name of the currently logged in user
   */
  public static String getUsername() {
    String username = null;
    Authentication auth = SecurityContextHolder.getContext().getAuthentication();
    if (auth != null) {
      if (auth.getPrincipal() instanceof UserDetails) {
        username = ((UserDetails) auth.getPrincipal()).getUsername();
      } else {
        username = auth.getPrincipal().toString();
      }
    }
    return username;
  }
}
