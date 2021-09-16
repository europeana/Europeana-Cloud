package eu.europeana.cloud.common.utils;

import java.time.Instant;

public class Clock {

    private Clock(){}

    public static long millisecondsSince(Instant start){
        return Instant.now().toEpochMilli() - start.toEpochMilli();
    }
}
