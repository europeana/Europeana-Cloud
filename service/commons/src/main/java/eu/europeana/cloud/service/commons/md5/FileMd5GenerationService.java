package eu.europeana.cloud.service.commons.md5;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Generates md5 hash from the provided file. Result is in HEX.
 */
public final class FileMd5GenerationService {

  private static final String HEX_NUMBER_REGEXP = "[0-9a-fA-F]*";
  private static final Pattern HEX_NUMBER_PATTER = Pattern.compile(HEX_NUMBER_REGEXP);

  private FileMd5GenerationService() {
  }

  public static UUID generateUUID(Path filePath) throws IOException {
    return generateUUID(Files.readAllBytes(filePath));
  }

  public static UUID generateUUID(byte[] fileBytes) {
    return UUID.nameUUIDFromBytes(fileBytes);
  }

  public static UUID md5ToUUID(byte[] md5digest) {
    if (md5digest == null) {
      throw new NullPointerException("md5digest cannot be null");
    } else if (md5digest.length != 16) {
      throw new IllegalArgumentException("md5digest has to have 16 bytes");
    }

    byte[] md5Bytes = Arrays.copyOf(md5digest, md5digest.length);

    md5Bytes[6] = (byte) (md5Bytes[6] & 15);
    md5Bytes[6] = (byte) (md5Bytes[6] | 48);
    md5Bytes[8] = (byte) (md5Bytes[8] & 63);
    md5Bytes[8] = (byte) (md5Bytes[8] | 128);

    long msb = 0L;
    long lsb = 0L;

    int i;
    for (i = 0; i < 8; ++i) {
      msb = (msb << 8) | (long) (md5Bytes[i] & 255);
    }

    for (i = 8; i < 16; ++i) {
      lsb = (lsb << 8) | (long) (md5Bytes[i] & 255);
    }

    return new UUID(msb, lsb);
  }

  public static UUID md5ToUUID(String md5digestInHex) {
    if (md5digestInHex == null) {
      throw new NullPointerException("md5digestInHex cannot be null");
    } else if (md5digestInHex.length() != 16 * 2) {
      throw new IllegalArgumentException("md5digestInHex has to have 16 bytes");
    } else if (!HEX_NUMBER_PATTER.matcher(md5digestInHex).matches()) {
      throw new IllegalArgumentException("md5digestInHex is not a valid hexadecimal number");
    }

    byte[] md5digest = new byte[16];

    for (int index = 0; index < 16; index++) {
      md5digest[index] = (byte) Integer.parseInt(md5digestInHex.substring(2 * index, 2 * index + 2), 16);
    }

    return md5ToUUID(md5digest);
  }

}
