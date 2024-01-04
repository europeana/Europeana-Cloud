package eu.europeana.cloud.service.dps.storm.utils;

import java.nio.charset.StandardCharsets;
import org.apache.commons.codec.digest.MurmurHash3;

public final class BucketUtils {

  private BucketUtils() {
  }

  public static int bucketNumber(String key, int bucketCount) {
    return (bucketCount - 1) & hash(key);
  }

  //WARNING: This hashcode is used as partition key in Cassandra, so after any change in method behaviour data in
  // Cassandra would be corrupted!
  // It also could be in case of updating library apache commons-codecs. So change log of library should be verified.
  // In any case test: eu.europeana.cloud.service.dps.storm.utils.BucketUtilsTest.testHashFuctionUnchanged()
  // could be used to check if function works properly.
  public static int hash(String key) {
    return MurmurHash3.hash32x86(key.getBytes(StandardCharsets.UTF_8));
  }
}
