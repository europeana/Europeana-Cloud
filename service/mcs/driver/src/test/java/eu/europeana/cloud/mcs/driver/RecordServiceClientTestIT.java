package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

@Ignore
public class RecordServiceClientTestIT {
//http://localhosta:8080/mcs/records/2YRDQTLJMPCN264Y75CVIJ65RPZP5DJFS36CYAGMNIGT3GLKLMDA/representations/edm/versions
//https://test.ecloud.psnc.pl/api/records/2YRDQTLJMPCN264Y75CVIJ65RPZP5DJFS36CYAGMNIGT3GLKLMDA/representations/edm/versions

//https://test.ecloud.psnc.pl/api/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA/representations/metadataRecord/versions/
//http://localhost:8080/mcs/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA/representations/metadataRecord/versions/



    //private static final Logger LOGGER = LoggerFactory.getLogger(RecordServiceClientTestIT.class);

    private  static final String LOCAL_TEST_URL = "http://localhost:8080/mcs";
    private  static final String LOCAL_TEST_UIS_URL = "http://localhost:8080/uis";
    private  static final String REMOTE_TEST_URL = "https://test.ecloud.psnc.pl/api";
    private  static final String REMOTE_TEST_UIS_URL = "https://test.ecloud.psnc.pl/api";

    private static final String SUPER_USERNAME = "metis_test";  //user z bazy danych
    private static final String SUPER_PASSWORD = "1RkZBuVf";
    private static final String PROVIDER_ID = "xxx";
    private static final String DATA_SET_ID = "DATA_SET_ID";

    //http://localhost:8080/mcs/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA
    //https://test.ecloud.psnc.pl/api/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA
    @Test
    public void getRecord() throws MCSException {
        String cloudId = "SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA";

        RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, null, null);
        Record record = mcsClient.getRecord(cloudId);
        assertThat(record.getCloudId(), is(cloudId));
    }


    @Test
    public void deleteRecord() throws MCSException {
        String cloudId = "SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA";

        RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, null, null);
        mcsClient.deleteRecord(cloudId);
    }

    //http://localhost:8080/mcs/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA/representations
    //https://test.ecloud.psnc.pl/api/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA/representations
    @Test
    public void getRepresentations() throws MCSException {
        String cloudId = "SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA";

        RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, SUPER_USERNAME, SUPER_PASSWORD);
        List<Representation> representations = mcsClient.getRepresentations(cloudId);
        assertThat(representations.size(), is(1));
        assertThat(representations.get(0).getCloudId(), is(cloudId));
    }

    //http://localhost:8080/mcs/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA/representations/metadataRecord
    //https://test.ecloud.psnc.pl/api/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA/representations/metadataRecord
    @Test
    public void getRepresentation() throws MCSException {
        String cloudId = "SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA";

        RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, SUPER_USERNAME, SUPER_PASSWORD);
        Representation representation = mcsClient.getRepresentation(cloudId, "metadataRecord");

        assertThat(representation.getCloudId(), is(cloudId));
    }

    @Test
    public void createRepresentation() throws CloudException, MCSException {
        String representationName = "StrangeRepresentationName";

        UISClient uisClient = new UISClient(REMOTE_TEST_UIS_URL, SUPER_USERNAME, SUPER_PASSWORD);
        CloudId cloudId = uisClient.createCloudId(PROVIDER_ID);

        RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, SUPER_USERNAME, SUPER_PASSWORD);
        URI representationURI = mcsClient.createRepresentation(cloudId.getId(), representationName, PROVIDER_ID);

        int index = representationURI.toString().indexOf("/records/" + cloudId.getId() + "/representations/" + representationName + "/versions/");
        assertThat(index, not(-1));
    }

/*
    public URI createRepresentation(String cloudId,
                                    String representationName,
                                    String providerId,
                                    InputStream data,
                                    String fileName,
                                    String mediaType) throws IOException, MCSException {
*/


/*
    public URI createRepresentation(String cloudId,
                                    String representationName,
                                    String providerId,
                                    InputStream data,
                                    String fileName,
                                    String mediaType,
                                    String key, String value) throws IOException, MCSException {
*/


/*
    public URI createRepresentation(String cloudId,
                                    String representationName,
                                    String providerId,
                                    InputStream data,
                                    String mediaType) throws IOException, MCSException {
*/



//    public void deleteRepresentation(String cloudId, String representationName)


    //http://localhost:8080/mcs/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA/representations
    //https://test.ecloud.psnc.pl/api/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA/representations
    @Test
    public void getRepresentationsWithName() throws MCSException {
        String cloudId = "SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA";

        RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, SUPER_USERNAME, SUPER_PASSWORD);
        List<Representation> representations = mcsClient.getRepresentations(cloudId, "metadataRecord");
        assertThat(representations.size(), is(2));

        for(Representation representation : representations) {
            assertThat(representation.getCloudId(), is(cloudId));
        }
    }


}
