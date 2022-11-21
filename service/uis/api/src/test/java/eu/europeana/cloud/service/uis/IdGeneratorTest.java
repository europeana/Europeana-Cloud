package eu.europeana.cloud.service.uis;

import eu.europeana.cloud.service.uis.encoder.IdGenerator;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

public class IdGeneratorTest {

  /**
   * Encode collision test. Related to jira issue ECL-392. Test might took long time and resource.
   */
  @Test
  @Ignore
  public void encodeCollisionTest() {
    // given
    Map<String, String> map = new HashMap<String, String>();
    for (BigInteger bigCounter = BigInteger.ONE; bigCounter
        .compareTo(new BigInteger("5000000")) < 0; bigCounter = bigCounter
        .add(BigInteger.ONE)) {
      final String counterString = bigCounter.toString(32);

      // when
      final String encodedId = IdGenerator.encodeWithSha256AndBase32(counterString);
      if (map.containsKey(encodedId)) {

        // then
        fail("bigCounter: " + bigCounter + " | counterString: "
            + counterString + " | encodedId:" + encodedId
            + " == collision with ==> " + map.get(encodedId));
      } else {
        map.put(encodedId, "bigCounter: " + bigCounter
            + " | counterString: " + counterString
            + " | encodedId:" + encodedId);
      }

    }

  }

  @Test
  public void encode_generateTheSameId() {
    // given
    final String testStr = "123456789012345";
    // when
    final String id1 = IdGenerator.encodeWithSha256AndBase32(testStr);
    final String id2 = IdGenerator.encodeWithSha256AndBase32(testStr);
    // then
    Assert.assertEquals(id1, id2);
  }

  @Test
  public void encode_generateDiffrendId() {
    // given
    final String testStr = "123456789012345";
    // when
    final String id1 = IdGenerator.encodeWithSha256AndBase32(testStr);
    final String id2 = IdGenerator.encodeWithSha256AndBase32(testStr + "additional");
    // then
    Assert.assertNotSame(id1, id2);
  }

  @Test
  public void timeEncode_generateDiffrendId() throws InterruptedException {
    // given
    final String testStr = "123456789012345";
    for (int i = 0; i < 100; i++) {
      // when
      final String id1 = IdGenerator.timeEncode(testStr);
      final String id2 = IdGenerator.timeEncode(testStr);
      // then
      if (id1.equals(id2)) {
        fail();
      }
    }
  }

}
