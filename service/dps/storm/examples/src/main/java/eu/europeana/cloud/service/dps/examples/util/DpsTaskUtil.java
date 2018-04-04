package eu.europeana.cloud.service.dps.examples.util;

import com.google.common.collect.Lists;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import net.iharder.Base64;

import java.text.SimpleDateFormat;
import java.util.*;

import static eu.europeana.cloud.service.dps.examples.toplologies.constants.TopologyConstants.ECLOUD_MCS_PASSWORD;
import static eu.europeana.cloud.service.dps.examples.toplologies.constants.TopologyConstants.ECLOUD_MCS_USERNAME;

/**
 * dps Task helpers
 *
 * @author manos
 */
public class DpsTaskUtil {

    // number of data sets to be included
    private static final int DATASETS_COUNT = 1;
    private static final String DATASET_URL_FOR_XSLT = "http://localhost:8080/mcs/data-providers/provider/data-sets/XSltDataset";
    private static final String XSLT_URL = "http://localhost:8080/mcs/records/EU6ZTUYQRZWDQTDHIQUPYQHZUSHG2IXU5RCP4F5SBSLQ25QFNJ7Q/representations/TarekRep/versions/a73d68b0-c06c-11e7-88d5-1c6f653f9042/files/00b863a9-2215-461a-9111-dcde2352c3bc";
    private static final String REVISION_NAME = "REVISION_NAME";
    private static final String REVISION_PROVIDER = "Tiff_tarek_final";

    private static final String DATASET_URL_FOR_IC = "http://localhost:8080/mcs/data-providers/provider/data-sets/dataset4";
    private static final String REPOSITORY_URL = "http://lib.psnc.pl/dlibra/oai-pmh-repository.xml";
    public static final String OUTPUT_DATASET = "http://localhost:8080/mcs/data-providers/provider/data-sets/dataset3";

    private static final String REPOSITORY_URL_FOR_HTTP = "http://ftp.eanadev.org/schema_zips/europeana_schemas.zip";


    /**
     * @return {@link DpsTask}
     */
    public static DpsTask generateDpsTaskForXSLT() {
        return generateDpsTaskForXSLT(DATASET_URL_FOR_XSLT, XSLT_URL, DATASETS_COUNT);
    }

    public static DpsTask generateDpsTaskForXSLT(final String dataSetURL, final String xslt, final int recordCount) {

        DpsTask task = createDpsTask(dataSetURL, recordCount);
        task.addParameter(PluginParameterKeys.XSLT_URL, xslt);
        task.addParameter(PluginParameterKeys.OUTPUT_DATA_SETS, "http://localhost:8080/mcs/data-providers/provider/data-sets/XSltDataset");

        return task;
    }

    public static DpsTask generateDPSTaskForIC(final String dataSetURL, final int recordCount) {

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
        task.addDataEntry(InputDataType.DATASET_URLS, dataSets);
        task.addParameter(PluginParameterKeys.AUTHORIZATION_HEADER, authorizationHeader);
        return task;
    }

    public static DpsTask generateDPSTaskForOAI() {
        DpsTask task = buildDPSHarvestingTask(REPOSITORY_URL);
        Map<String, String> parameters = buildHarvestingTaskParameter();
        parameters.put("INTERVAL", "31540000000");
        task.setParameters(parameters);
        OAIPMHHarvestingDetails harvestingDetails = configureHarvesterDetails();
        task.setHarvestingDetails(harvestingDetails);
        return task;

    }

    public static DpsTask generateDPSTaskForHTTP() {
        DpsTask task = buildDPSHarvestingTask(REPOSITORY_URL_FOR_HTTP);
        Map<String, String> parameters = buildHarvestingTaskParameter();
        task.setParameters(parameters);
        return task;

    }

    private static DpsTask buildDPSHarvestingTask(String repositoryURL) {
        DpsTask task = new DpsTask();
        List<String> urls = new ArrayList<>();
        urls.add(repositoryURL);
        task.addDataEntry(InputDataType.REPOSITORY_URLS, urls);
        return task;
    }

    private static Map<String, String> buildHarvestingTaskParameter() {
        Map<String, String> parameters = new HashMap<>();
        parameters.put("PROVIDER_ID", "provider");
        parameters.put(PluginParameterKeys.NEW_REPRESENTATION_NAME, "NEW_REPRESENTATION_NAME");
        parameters.put(PluginParameterKeys.OUTPUT_DATA_SETS, OUTPUT_DATASET);
        String authorizationHeader = "Basic " + Base64.encodeBytes((ECLOUD_MCS_USERNAME + ":" + ECLOUD_MCS_PASSWORD).getBytes());
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, authorizationHeader);
        return parameters;
    }


    private static OAIPMHHarvestingDetails configureHarvesterDetails() {
        OAIPMHHarvestingDetails harvestingDetails = new OAIPMHHarvestingDetails();
        Set requiredMetaDataFormats = new HashSet();
        requiredMetaDataFormats.add("oai_dc");
        harvestingDetails.setSchemas(requiredMetaDataFormats);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            harvestingDetails.setDateFrom(sdf.parse("2016-10-15"));
        } catch (Exception e) {

        }
        return harvestingDetails;
    }

    public static DpsTask generateDPSTaskForIC() {
        return generateDPSTaskForIC(DATASET_URL_FOR_IC, DATASETS_COUNT);
    }


}
