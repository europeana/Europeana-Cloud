package eu.europeana.cloud.service.mcs.controller;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import org.apache.commons.io.IOUtils;

/**
 * @author krystian.
 */
public class Helper {

  public static byte[] readFully(InputStream instance, int length) throws IOException {
    byte[] actual = new byte[length];
    IOUtils.readFully(instance, actual);
    return actual;
  }

  public static byte[] generateRandom(int size) {
    byte[] buf = new byte[size];
    new Random().nextBytes(buf);
    return buf;
  }

  public static byte[] generateSeq(int size) {
    byte[] expected;
    expected = new byte[size];
    for (int i = 0; i < expected.length; i++) {
      expected[i] = (byte) i;
    }
    return expected;
  }
}
