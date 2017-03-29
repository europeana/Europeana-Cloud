package eu.europeana.cloud.service.dps.examples.util;

import java.util.List;

import com.google.common.collect.Lists;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import net.iharder.Base64;

import static eu.europeana.cloud.service.dps.examples.toplologies.constants.TopologyConstants.*;

/**
 * dps Task helpers
 *
 * @author manos
 */
public class DpsTaskUtil {

    // number of data sets to be included
    private final static int DATASETS_COUNT = 1;

    private static String datasetURLForXSLT = "http://localhost:8080/mcs/data-providers/Tiff_tarek_final/data-sets/xsltDataset";
    private static String xsltURL = "http://localhost:8080/mcs/records/TAJZ2ZVNTXLQ6R5SMBY2MYKONUCFBFMPY2TCQNMA2ZL4CXHMATHA/representations/Tarek_Representation1/versions/29dc84f0-144b-11e7-87bb-1c6f653f9042/files/30e5772f-23e4-4910-af0f-ded2e5e883ee";
    private final static String REVISION_NAME = "REVISION_NAME";
    private final static String REVISION_PROVIDER = "Tiff_tarek_final";

    private static String datasetURLForIC = "http://localhost:8080/mcs/data-providers/Tiff_tarek_final/data-sets/ic_dataset";


    /**
     * @return  {@link DpsTask}
     */
    public static DpsTask generateDpsTaskForXSLT() {
        return generateDpsTaskForXSLT(datasetURLForXSLT, xsltURL, DATASETS_COUNT);
    }

    public static DpsTask generateDpsTaskForXSLT(final String dataSetURL, final String xslt, final int recordCount) {

        DpsTask task = createDpsTask(dataSetURL, recordCount);
        task.addParameter(PluginParameterKeys.XSLT_URL, xslt);
        task.addParameter(PluginParameterKeys.OUTPUT_URL, null);

        return task;
    }

    public static DpsTask generateDpsTaskForIc(final String dataSetURL, final int recordCount) {

        DpsTask task = createDpsTask(dataSetURL, recordCount);

        task.addParameter(PluginParameterKeys.OUTPUT_MIME_TYPE, "image/jp2");
        task.addParameter(PluginParameterKeys.MIME_TYPE, "image/tiff");
        return task;
    }

    private static DpsTask createDpsTask(String dataSetURL, int recordCount) {
        DpsTask task = new DpsTask();
        task.setOutputRevision(new Revision(REVISION_NAME, REVISION_PROVIDER));

        List<String> dataSets = Lists.newArrayList();
        for (int i = 0; i < recordCount; i++) {
            dataSets.add(dataSetURL);
        }

        String authorizationHeader = "Basic " + Base64.encodeBytes((ECLOUD_MCS_USERNAME + ":" + ECLOUD_MCS_PASSWORD).getBytes());
        task.addDataEntry(DpsTask.DATASET_URLS, dataSets);
        task.addParameter(PluginParameterKeys.AUTHORIZATION_HEADER, authorizationHeader);
        return task;
    }

    public static DpsTask generateDPsTaskForIC() {
        return generateDpsTaskForIc(datasetURLForIC, DATASETS_COUNT);
    }


}
