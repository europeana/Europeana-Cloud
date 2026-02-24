package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.service.commons.urls.DataSetUrlParser;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DpsTaskDataSetUtil {
    public static List<String> readDataSetUrlsList(DpsTask dpsTask) {
        String listParameter = dpsTask.getParameter(PluginParameterKeys.OUTPUT_DATA_SETS);
        return listParameter == null ?
                List.of() :
                Arrays.asList(listParameter.split(","));
    }

    private static DataSet parseDatasetFromUrl(String datasetUrl) throws MalformedURLException {
        return DataSetUrlParser.parse(datasetUrl);
    }

    public static List<DataSet> getDatasetOutOfDpsTask(DpsTask dpsTask) throws MalformedURLException {
        List<String> dataSetUrls = readDataSetUrlsList(dpsTask);
        List<DataSet> dataSets = new ArrayList<>();
        for (String dataSetUrl :
                dataSetUrls) {
            dataSets.add(parseDatasetFromUrl(dataSetUrl));

        }
        return dataSets;
    }
}
