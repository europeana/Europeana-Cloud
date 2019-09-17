package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.MCSException;

import java.util.List;

/**
 * Created by Tarek on 9/3/2019.
 */
public class Tarek {
    static String USER_NAME = "metis_production";
    static String PASSWORD = "MGJkYTFkMGQ5YWJ";
    static String MCS_URL = "http://150.254.164.4/mcs";
    static String PROVIDER_ID = "metis_production";

    public static void main(String[] args) throws Exception {

       /* {
            "inputData":{
            "DATASET_URLS":[
            "https://ecloud.psnc.pl/api//data-providers/metis_production/data-sets/1b19eac0-0e7d-4d42-83c1-28d42627f649"
            ]},"parameters":{
            "SCHEMA_NAME":"http://ftp.eanadev.org/schema_zips/europeana_schemas-20190627.zip",
                    "REVISION_PROVIDER":"metis_production",
                    "AUTHORIZATION_HEADER":"Basic bWV0aXNfcHJvZHVjdGlvbjpNR0prWVRGa01HUTVZV0o=",
                    "ROOT_LOCATION":"EDM.xsd", "SCHEMATRON_LOCATION":"schematron/schematron.xsl", "REVISION_NAME":
            "OAIPMH_HARVEST",
                    "OUTPUT_DATA_SETS":
            "https://ecloud.psnc.pl/api//data-providers/metis_production/data-sets/1b19eac0-0e7d-4d42-83c1-28d42627f649",
                    "NEW_REPRESENTATION_NAME":"metadataRecord", "REPRESENTATION_NAME":"metadataRecord",
                    "REVISION_TIMESTAMP":"2019-09-03T08:59:41.474Z",
                    "PREVIOUS_TASK_ID":"1919713277938451640"
        },"outputRevision":{
            "revisionName":"VALIDATION_EXTERNAL", "revisionProviderId":"metis_production"
                    , "creationTimeStamp":1567502613656, "published":false, "acceptance":false, "deleted":false
        },
            "taskId":-3470658088171254183, "taskName":"",
                "harvestingDetails":null
        }*/


       /*
       {"inputData":{"REPOSITORY_URLS":["http://sta-1418.gruppometa.it/oaipmh/"]},
       "parameters":{"AUTHORIZATION_HEADER":"Basic bWV0aXNfcHJvZHVjdGlvbjpNR0prWVRGa01HUTVZV0o=",
       "METIS_DATASET_ID":"115","OUTPUT_DATA_SETS":
       "https://ecloud.psnc.pl/api//data-providers/metis_production/data-sets/1b19eac0-0e7d-4d42-83c1-28d42627f649","NEW_REPRESENTATION_NAME":"metadataRecord","USE_DEFAULT_IDENTIFIERS":"false","PROVIDER_ID":"metis_production"},"outputRevision":{"revisionName":"OAIPMH_HARVEST","revisionProviderId":"metis_production","creationTimeStamp":1567501181474,"published":false,"acceptance":false,"deleted":false},"taskId":1919713277938451640,"taskName":"","harvestingDetails":{"schemas":["edm"],"excludedSchemas":null,"sets":["mibac.1418.oaipmh.sets.PaginaPeriodicoTrincea"],"excludedSets":null,"dateFrom":null,"dateUntil":null,"granularity":null}}

        */
        DataSetServiceClient dataSetServiceClient = new DataSetServiceClient(MCS_URL, USER_NAME, PASSWORD);

        String startFrom = null;
        int count = 0;
        do {
            ResultSlice<CloudTagsResponse> resultSlice = getDataSetRevisionsChunk(dataSetServiceClient, "metadataRecord", "OAIPMH_HARVEST", "metis_production", "2018-08-08T08:58:48.474Z", "metis_production", "1b19eac0-0e7d-4d42-83c1-28d42627f649", startFrom);
            List<CloudTagsResponse> cloudTagsResponses = resultSlice.getResults();
            for (CloudTagsResponse cloudTagsResponse : cloudTagsResponses) {
                System.out.println(cloudTagsResponse.getCloudId());
                count++;
            }
            startFrom = resultSlice.getNextSlice();
        }
        while (startFrom != null);
        System.out.println(count);
    }

    private static ResultSlice<CloudIdAndTimestampResponse> getLatestDataSetCloudIdByRepresentationAndRevisionChunk(DataSetServiceClient dataSetServiceClient, String representationName, String revisionName, String revisionProvider, String datasetName, String datasetProvider, String startFrom) throws MCSException, DriverException {
        ResultSlice<CloudIdAndTimestampResponse> resultSlice = dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevisionChunk(datasetName, datasetProvider, revisionProvider, revisionName, representationName, false, startFrom);
        if (resultSlice == null || resultSlice.getResults() == null) {
            throw new DriverException("Getting cloud ids and revision tags: result chunk obtained but is empty.");
        }
        return resultSlice;

    }

    private static ResultSlice<CloudTagsResponse> getDataSetRevisionsChunk(DataSetServiceClient dataSetServiceClient, String representationName, String revisionName, String revisionProvider, String revisionTimestamp, String datasetProvider, String datasetName, String startFrom) throws MCSException, DriverException {
        try {
            ResultSlice<CloudTagsResponse> resultSlice = dataSetServiceClient.getDataSetRevisionsChunk(datasetProvider, datasetName, representationName,
                    revisionName, revisionProvider, revisionTimestamp, startFrom, null);
            if (resultSlice == null || resultSlice.getResults() == null) {
                throw new DriverException("Getting cloud ids and revision tags: result chunk obtained but is empty.");
            }

            return resultSlice;
        } catch (Exception e) {
            return null;
        }

    }
}


