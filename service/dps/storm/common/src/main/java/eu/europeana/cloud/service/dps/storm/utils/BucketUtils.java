package eu.europeana.cloud.service.dps.storm.utils;

import org.apache.commons.codec.digest.MurmurHash3;

import java.nio.charset.StandardCharsets;

public class BucketUtils {

    static {
        String libVersion = MurmurHash3.class.getPackage().getImplementationVersion();
        if (!"1.15".equals(libVersion)) {
            //This exception is thrown to prevent unexpected change of hashCode implementation in apache library
            //which we use in hashFunction
            //This hashcode is used as partition key in Cassandra so, after its chage data would be corrupted.
            //After changing library version the method we should execute test method to check its behaviour
            // eu.europeana.cloud.service.dps.storm.utils.BucketUtilsTest.testHashFuctionUnchanged()
            throw new IllegalStateException("Detected chagned commons-coded library version: " +
                    libVersion + ". Hash fuction should be verified!");
        }
    }

    private BucketUtils() {
    }

    public static int bucketNumber(String key, int bucketCount) {
        return (bucketCount - 1) & hash(key);
    }

    public static int hash(String key) {
        return MurmurHash3.hash32x86(key.getBytes(StandardCharsets.UTF_8));
    }
}