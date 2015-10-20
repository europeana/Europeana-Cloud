package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.service.dps.storm.StormTaskTuple;

/**
 * Created by Tarek on 10/15/2015.
 */
public class TaskTupleUtility {
    //The template one
    private boolean isProvidedAsParameter(StormTaskTuple stormTaskTuple, String parameter) {
        if (stormTaskTuple.getParameter(parameter) != null) {
            return true;
        } else {
            return false;
        }
    }

    //The template one
    public String getParameterFromTuple(StormTaskTuple stormTaskTuple, String parameter) {
        String outputValue = parameter;
        if (isProvidedAsParameter(stormTaskTuple, parameter)) {
            outputValue = stormTaskTuple.getParameter(parameter);
        }
        return outputValue;
    }
}
