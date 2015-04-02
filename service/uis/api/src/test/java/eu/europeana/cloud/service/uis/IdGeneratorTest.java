package eu.europeana.cloud.service.uis;

import eu.europeana.cloud.service.uis.encoder.IdGenerator;
import static java.lang.Thread.sleep;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import static org.junit.Assert.fail;
import org.junit.Ignore;
import org.junit.Test;

public class IdGeneratorTest {

    /**
     * Encode collision test. Related to jira issue ECL-392. Test might took
     * long time and resource.
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
	    final String encodedId = IdGenerator.encode(counterString);
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
	final String id1 = IdGenerator.encode(testStr);
	final String id2 = IdGenerator.encode(testStr);
	// then
	Assert.assertEquals(id1, id2);
    }

    @Test
    public void encode_generateDiffrendId() {
	// given
	final String testStr = "123456789012345";
	// when
	final String id1 = IdGenerator.encode(testStr);
	final String id2 = IdGenerator.encode(testStr + "additional");
	// then
	Assert.assertNotSame(id1, id2);
    }

    @Test
    public void timeEncode_generateDiffrendId() throws InterruptedException {
	// given
	final String testStr = "123456789012345";
	// when
	final String id1 = IdGenerator.timeEncode(testStr);
	sleep(1);
	final String id2 = IdGenerator.timeEncode(testStr);
	// then
	Assert.assertNotSame(id1, id2);
    }

}
