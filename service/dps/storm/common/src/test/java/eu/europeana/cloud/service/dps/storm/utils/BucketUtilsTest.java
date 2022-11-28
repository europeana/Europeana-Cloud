package eu.europeana.cloud.service.dps.storm.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.google.common.io.Resources;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.commons.io.Charsets;
import org.junit.Test;

public class BucketUtilsTest {

  @Test
  //WARNING This hash function is used as partition key in Cassandra, so after any change in method behaviour data in
  // Cassandra would be corrupted.
  public void shouldHashFunctionBeUnchanged() throws IOException {
    List<String> strings = Resources.readLines(Resources.getResource("random_strings.txt"), Charsets.UTF_8);
    List<String> hashes = Resources.readLines(Resources.getResource("string_hashcodes.txt"), Charsets.UTF_8);

    assertEquals(strings.size(), hashes.size());
    assertFalse(strings.isEmpty());
    for (int i = 0; i < strings.size(); i++) {
      assertEquals("Bad hash for string in line number " + i, Integer.parseInt(hashes.get(i)), BucketUtils.hash(strings.get(i)));
    }
  }

  //method to generate file with hash code - could be executed by hand
  public void regenerateHashes() throws IOException {
    try (FileWriter out = new FileWriter("/home/user/dir/string_hashcodes.txt", StandardCharsets.UTF_8)) {
      for (String string : Resources.readLines(Resources.getResource("random_strings.txt"), Charsets.UTF_8)) {
        out.write("" + BucketUtils.hash(string));
        out.write("\n");
      }
    }
  }
}