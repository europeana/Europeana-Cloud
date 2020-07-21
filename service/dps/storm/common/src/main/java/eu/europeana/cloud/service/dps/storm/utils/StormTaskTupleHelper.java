package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.service.dps.storm.StormTaskTuple;

public class StormTaskTupleHelper {

    private StormTaskTupleHelper() {
    }

    public static boolean isMessageResent(StormTaskTuple tuple) {
        return tuple.getRecordAttemptNumber() > 0;
    }
}
