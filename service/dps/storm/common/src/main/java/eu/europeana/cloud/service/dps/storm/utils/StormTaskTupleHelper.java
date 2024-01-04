package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.service.commons.urls.DataSetUrlParser;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import java.net.MalformedURLException;

public final class StormTaskTupleHelper {

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

  public static String extractDatasetId(StormTaskTuple tuple) throws MalformedURLException {
    DataSet dataset = DataSetUrlParser.parse(tuple.getParameter(PluginParameterKeys.OUTPUT_DATA_SETS));
    return dataset.getId();
  }

}
