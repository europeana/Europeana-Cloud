package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;

public class StormTaskTupleHelper {

    private StormTaskTupleHelper() {
    }

    public static boolean isMessageResent(StormTaskTuple tuple) {
        return tuple.getRecordAttemptNumber() > 1;
    }

    public static boolean statisticsShouldBeGenerated(StormTaskTuple tuple) {
        String parameter = tuple.getParameter(PluginParameterKeys.GENERATE_STATS);
        return parameter == null || "true".equalsIgnoreCase(parameter);
    }

    public static long getRecordProcessingStartTime(StormTaskTuple tuple) {
        return Long.parseLong(tuple.getParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS));
    }
}
