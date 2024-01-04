package eu.europeana.cloud.service.uis.encoder;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.codec.binary.Base32;

/**
 * IdGenerator generate unique id. The characters consists of Base32 dictionary. The implementation ensures that the same input
 * will generate the same result and that the output will always be 52 characters.
 */
public final class IdGenerator {

  private IdGenerator() {
  }

  /**
   * Encode a given string and timestamp.
   *
   * @param input The string to encode
   * @return A 52 character encoded version of the String representation
   */
  public static String timeEncode(final String input) {
    return encodeWithSha256AndBase32(input + System.nanoTime());
  }


  /**
   * Encode a given string. Encodes the input with SHA 256 and Base32.
   *
   * @param input The string to encode
   * @return A 52 character encoded version of the String representation
   */
  public static String encodeWithSha256AndBase32(String input) {
    byte[] digest = null;
    try {
      final MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update(input.getBytes(StandardCharsets.UTF_8));
      digest = md.digest();
    } catch (NoSuchAlgorithmException ex) {
      throw new RuntimeException(ex);
    }

    final Base32 base32 = new Base32();
    //substring removes padding
    return base32.encodeAsString(digest).substring(0, 52);
  }
}
