package eu.europeana.cloud.service.dps.storm.utils;

import com.google.common.hash.Hashing;
import org.apache.commons.io.Charsets;

public class BucketUtils {

    public static int bucketNumber(String key, int bucketCount) {
        return (bucketCount - 1) &
                Hashing.murmur3_32().hashString(key, Charsets.UTF_8).asInt();
    }
}
