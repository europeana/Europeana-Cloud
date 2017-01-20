package eu.europeana.cloud.mcs.driver;

/**
 * Created by Tarek on 1/16/2017.
 */

import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.utils.Tags;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Test {

    private static String userName = "admin";
    private static String password = "admin";
    private static String baseUrl = "http://localhost:8080/mcs";
    private static String cloudId = "TAJZ2ZVNTXLQ6R5SMBY2MYKONUCFBFMPY2TCQNMA2ZL4CXHMATHA";
    private static String providerId = "Tiff_tarek_final";
    private static String representationName = "Tarek_Representation1";
    private static String versionId = "5c7d0c60-dbc8-11e6-810f-1c6f653f9042";
    private static String datasetId = "tarekDatasetTwo";
    private static RecordServiceClient recordServiceClient;
    private static RevisionServiceClient revisionServiceClient;


    private static String cloudId2 = "ZNZONGRIDBK4XMKEGQBV73CEO27RAAVDOWB3OQEH4GXEJO46SOHQ";
    private static String providerId2 = "t_prov";
    private static String getRepresentationName2 = "tarekReptttt";
    private static String version2 = "99198df0-dbd3-11e6-b006-1c6f653f9042";


    private static String cloudId3 = "Q2TDCTPNSX5BLPL2PGBFC6M3NRLZLHEREFZ6427USFFFNXY3DC2Q";
    private static String providerId3 = "A-PROVIDER";
    private static String version3 = "d5b7a5d0-df02-11e6-8829-1c6f653f9042";



    public static void main(String[] args) throws Exception {
        recordServiceClient = new RecordServiceClient(baseUrl, userName, password);
        revisionServiceClient = new RevisionServiceClient(baseUrl, userName, password);
        DataSetServiceClient dataSetServiceClient = new DataSetServiceClient(baseUrl, userName, password);

      //    System.out.println(recordServiceClient.createRepresentation(cloudId3, representationName, providerId));

        //   System.out.println(dataSetServiceClient.createDataSet(providerId2, "tarekDatasetTwo", "dsdsd"));
        dataSetServiceClient.assignRepresentationToDataSet(providerId, datasetId, cloudId3, representationName, version3);
        //     System.out.println(recordServiceClient.getRepresentation(cloudId, representationName, "536586b0-58bb-11e6-91e3-1c6f653f9042"));


        //  System.out.println(revisionServiceClient.addRevision(cloudId, representationName, "a3cbb130-5ed1-11e6-a45a-1c6f653f9042", "Revision1", providerId, "deleted"));
        Revision revision = new Revision("lalallala", providerId2);
        Set<Tags> tags = new HashSet<>();
        tags.add(Tags.DELETED);
        tags.add(Tags.ACCEPTANCE);

        System.out.println(revisionServiceClient.addRevision(cloudId3, representationName, version3, "torakawewewewewewwww", providerId2, tags));


        ResultSlice<CloudIdAndTimestampResponse> responsed = dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevisionChunk(datasetId, providerId, providerId2, "torakawewewewewewwww", representationName, null);
        List<CloudIdAndTimestampResponse> results = responsed.getResults();
        System.out.println(results.size());
        System.out.println(results.get(0));
        System.out.println("*********************");
        results = dataSetServiceClient.getLatestDataSetCloudIdByRepresentationAndRevision(datasetId, providerId, providerId2, "torakawewewewewewwww", representationName);
        System.out.println(results.size());
        System.out.println(results.get(0));

    }
}

