package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.service.commons.urls.DataSetUrlParser;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;

import java.net.MalformedURLException;

public final class StormTaskTupleHelper {

  private StormTaskTupleHelper() {
  }

  public static boolean isMessageResent(CommonTaskTuple tuple) {
    return tuple.getRecordAttemptNumber() > 1;
  }

  public static boolean statisticsShouldBeGenerated(CommonTaskTuple tuple) {
    String parameter = tuple.getParameter(PluginParameterKeys.GENERATE_STATS);
    return parameter == null || "true".equalsIgnoreCase(parameter);
  }
}
