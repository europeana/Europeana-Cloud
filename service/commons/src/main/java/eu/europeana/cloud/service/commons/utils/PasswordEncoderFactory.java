package eu.europeana.cloud.service.commons.utils;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

public final class PasswordEncoderFactory {

  private PasswordEncoderFactory() {
  }

  public static PasswordEncoder getPasswordEncoder() {
    return new BCryptPasswordEncoder(4);
  }
}
