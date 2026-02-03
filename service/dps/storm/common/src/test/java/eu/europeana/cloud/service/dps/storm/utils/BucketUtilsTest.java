package eu.europeana.cloud.service.dps.storm.utils;

import com.google.common.io.Resources;
import org.apache.commons.io.Charsets;
import org.junit.jupiter.api.Test;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class BucketUtilsTest {

  @Test
    //WARNING This hash function is used as partition key in Cassandra, so after any change in method behaviour data in
    // Cassandra would be corrupted.
  void shouldHashFunctionBeUnchanged() throws IOException {
    List<String> strings = Resources.readLines(Resources.getResource("random_strings.txt"), Charsets.UTF_8);
    List<String> hashes = Resources.readLines(Resources.getResource("string_hashcodes.txt"), Charsets.UTF_8);

    assertEquals(strings.size(), hashes.size());
    assertFalse(strings.isEmpty());
    for (int i = 0; i < strings.size(); i++) {
      assertEquals(Integer.parseInt(hashes.get(i)), BucketUtils.hash(strings.get(i)), "Bad hash for string in line number " + i);
    }
  }

  //method to generate file with hash code - could be executed by hand
  void regenerateHashes() throws IOException {
    try (FileWriter out = new FileWriter("/home/user/dir/string_hashcodes.txt", StandardCharsets.UTF_8)) {
      for (String string : Resources.readLines(Resources.getResource("random_strings.txt"), Charsets.UTF_8)) {
        out.write("" + BucketUtils.hash(string));
        out.write("\n");
      }
    }
  }
}