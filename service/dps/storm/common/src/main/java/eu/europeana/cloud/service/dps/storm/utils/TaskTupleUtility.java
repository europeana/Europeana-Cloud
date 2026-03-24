package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;

/**
 * Created by Tarek on 10/15/2015.
 */
public final class TaskTupleUtility {

  private TaskTupleUtility() {
  }

  //check it parameter is provided in CommonTaskTuple
  protected static boolean isProvidedAsParameter(CommonTaskTuple commonTaskTuple, String parameter) {
    return commonTaskTuple.getParameter(parameter) != null;
  }

  //if the parameter was provided explicity in CommonTaskTuple its value will be returned or its default value provided in PluginParameterKeys will be returned
  public static String getParameterFromTuple(CommonTaskTuple commonTaskTuple, String parameter) {
    String outputValue = PluginParameterKeys.PLUGIN_PARAMETERS.get(parameter);
    if (isProvidedAsParameter(commonTaskTuple, parameter)) {
      outputValue = commonTaskTuple.getParameter(parameter);
    }
    return outputValue;
  }
}
