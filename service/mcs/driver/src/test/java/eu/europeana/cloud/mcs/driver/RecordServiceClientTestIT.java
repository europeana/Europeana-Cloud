package eu.europeana.cloud.mcs.driver;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import org.junit.Test;
import org.springframework.http.MediaType;

public class RecordServiceClientTestIT {
  //http://localhosta:8080/mcs/records/2YRDQTLJMPCN264Y75CVIJ65RPZP5DJFS36CYAGMNIGT3GLKLMDA/representations/edm/versions
  //https://test.ecloud.psnc.pl/api/records/2YRDQTLJMPCN264Y75CVIJ65RPZP5DJFS36CYAGMNIGT3GLKLMDA/representations/edm/versions

  //https://test.ecloud.psnc.pl/api/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA/representations/metadataRecord/versions/
  //http://localhost:8080/mcs/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA/representations/metadataRecord/versions/

  //private static final Logger LOGGER = LoggerFactory.getLogger(RecordServiceClientTestIT.class);

  private static final String LOCAL_TEST_URL = "http://localhost:8080/mcs";
  private static final String LOCAL_TEST_UIS_URL = "http://localhost:8080/uis";
  private static final String REMOTE_TEST_UIS_URL = "https://test.ecloud.psnc.pl/api";

  private static final String USER_NAME = "metis_test";  //user z bazy danych
  private static final String USER_PASSWORD = "1RkZBuVf";
  private static final String ADMIN_NAME = "admin";  //admin z bazy danych
  private static final String ADMIN_PASSWORD = "glEumLWDSVUjQcRVswhN";

  private static final String PROVIDER_ID = "xxx";
  private static final UUID VERSION = UUID.fromString("40585a42-e606-11eb-ba80-0242ac130004");
  private static final String DATASET_ID = "xxx";

  @Test
  public void getRecord() throws MCSException {
    String cloudId = "SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA";

    RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, null, null);
    Record record = mcsClient.getRecord(cloudId);
    assertThat(record.getCloudId(), is(cloudId));
  }


  @Test(expected = RecordNotExistsException.class)
  public void deleteRecord() throws CloudException, MCSException, IOException {
    String representationName = "StrangeRepresentationNameToDelete";
    String versionTemplate = "versions/";

    UISClient uisClient = new UISClient(REMOTE_TEST_UIS_URL, USER_NAME, USER_PASSWORD);
    CloudId cloudId = uisClient.createCloudId(PROVIDER_ID);

    RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, ADMIN_NAME, ADMIN_PASSWORD);

    String filename = "log4j.properties";
    String mediatype = MediaType.TEXT_PLAIN_VALUE;
    InputStream is = RecordServiceClientTestIT.class.getResourceAsStream("/" + filename);

    URI representationURI = mcsClient.createRepresentation(cloudId.getId(), representationName, PROVIDER_ID,
        DATASET_ID, is, filename, mediatype);

    String representationURIString = representationURI.toString();

    int versionIndex = representationURIString.indexOf(versionTemplate);
    representationURIString.substring(versionIndex + versionTemplate.length());

    mcsClient.getRepresentation(cloudId.getId(), representationName);

    Record record1 = mcsClient.getRecord(cloudId.getId());
    assertNotNull(record1);

    mcsClient.deleteRecord(cloudId.getId());

    mcsClient.getRecord(cloudId.getId());
  }

  //http://localhost:8080/mcs/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA/representations
  //https://test.ecloud.psnc.pl/api/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA/representations
  @Test
  public void getRepresentations1() throws MCSException {
    String cloudId = "SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA";

    RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
    List<Representation> representations = mcsClient.getRepresentations(cloudId);
    assertThat(representations.size(), is(1));
    assertThat(representations.get(0).getCloudId(), is(cloudId));
  }

  @Test
  public void getRepresentations2() throws MCSException {
    String cloudId = "SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA";
    String representationName = "metadataRecord";

    RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
    List<Representation> representations = mcsClient.getRepresentations(cloudId, representationName);
    assertThat(representations.size(), is(2));
    assertThat(representations.get(0).getCloudId(), is(cloudId));
  }

  //http://localhost:8080/mcs/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA/representations/metadataRecord
  //https://test.ecloud.psnc.pl/api/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA/representations/metadataRecord
  @Test
  public void getRepresentation() throws MCSException {
    String cloudId = "SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA";
    //String cloudId = "222B5I4VPV3XN43PZMD2UHC6NPA6B2ZY7ZRPQV2UUVXRHFDALXEA";

    String representationName = "metadataRecord";

    RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
    Representation representation = mcsClient.getRepresentation(cloudId, representationName);

    assertThat(representation.getCloudId(), is(cloudId));
  }


  //http://localhost:8080/mcs/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA/representations/metadataRecord
  //https://test.ecloud.psnc.pl/api/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA/representations/metadataRecord
  @Test
  public void getRepresentationKeyValue() throws MCSException {
    String cloudId = "SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA";
    String representationName = "metadataRecord";
    String version = "5a259500-392f-11ea-9718-fa163e64bb83";

    RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL);
    Representation representation = mcsClient.getRepresentation(cloudId, representationName, version);

    assertThat(representation.getCloudId(), is(cloudId));
  }


  @Test
  public void createRepresentation() throws CloudException, MCSException {
    String representationName = "StrangeRepresentationName";

    UISClient uisClient = new UISClient(REMOTE_TEST_UIS_URL, USER_NAME, USER_PASSWORD);
    CloudId cloudId = uisClient.createCloudId(PROVIDER_ID);

    RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
    URI representationURI = mcsClient.createRepresentation(cloudId.getId(), representationName, PROVIDER_ID,
        DATASET_ID);

    int index = representationURI.toString().indexOf(
        "/records/" + cloudId.getId() + "/representations/" + representationName + "/versions/");
    assertThat(index, not(-1));
  }


  @Test
  public void createRepresentationFile() throws CloudException, MCSException, IOException {
    String representationName = "StrangeRepresentationName";

    UISClient uisClient = new UISClient(REMOTE_TEST_UIS_URL, USER_NAME, USER_PASSWORD);
    CloudId cloudId = uisClient.createCloudId(PROVIDER_ID);

    String filename = "log4j.properties";
    String mediatype = MediaType.TEXT_PLAIN_VALUE;
    InputStream is = RecordServiceClientTestIT.class.getResourceAsStream("/" + filename);

    RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
    URI representationURI = mcsClient.createRepresentation(cloudId.getId(), representationName, PROVIDER_ID,
        DATASET_ID, is, filename, mediatype);

    int index = representationURI.toString().indexOf(
        "/records/" + cloudId.getId() + "/representations/" + representationName + "/versions/");
    assertThat(index, not(-1));
  }


  @Test
  public void createRepresentationFileKeyValue() throws CloudException, MCSException, IOException {
    String representationName = "StrangeRepresentationName";

    UISClient uisClient = new UISClient(REMOTE_TEST_UIS_URL, USER_NAME, USER_PASSWORD);
    CloudId cloudId = uisClient.createCloudId(PROVIDER_ID);

    String filename = "log4j.properties";
    String mediatype = MediaType.TEXT_PLAIN_VALUE;
    InputStream is = RecordServiceClientTestIT.class.getResourceAsStream("/" + filename);

    RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
    URI representationURI = mcsClient.createRepresentation(cloudId.getId(), representationName, PROVIDER_ID,
        DATASET_ID, is, filename, mediatype);

    int index = representationURI.toString().indexOf(
        "/records/" + cloudId.getId() + "/representations/" + representationName + "/versions/");
    assertThat(index, not(-1));
  }

  @Test
  public void createRepresentationFileNoName() throws CloudException, MCSException, IOException {
    String representationName = "StrangeRepresentationName";

    UISClient uisClient = new UISClient(REMOTE_TEST_UIS_URL, USER_NAME, USER_PASSWORD);
    CloudId cloudId = uisClient.createCloudId(PROVIDER_ID);

    String filename = "log4j.properties";
    String mediatype = MediaType.TEXT_PLAIN_VALUE;
    InputStream is = RecordServiceClientTestIT.class.getResourceAsStream("/" + filename);

    RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
    URI representationURI = mcsClient.createRepresentation(cloudId.getId(), representationName, PROVIDER_ID,
        DATASET_ID, is, mediatype);

    int index = representationURI.toString().indexOf(
        "/records/" + cloudId.getId() + "/representations/" + representationName + "/versions/");
    assertThat(index, not(-1));
  }

  @Test
  public void createRepresentationInGivenVersionKeyValue() throws CloudException, MCSException {
    String representationName = "StrangeRepresentationName";

    UISClient uisClient = new UISClient(LOCAL_TEST_UIS_URL, USER_NAME, USER_PASSWORD);
    //CloudId cloudId = uisClient.createCloudId(PROVIDER_ID, "recordt1");
    CloudId cloudId = uisClient.getCloudId(PROVIDER_ID, "recordt1");

    RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
    URI representationURI = mcsClient.createRepresentation(cloudId.getId(), representationName, PROVIDER_ID, VERSION,
        DATASET_ID);

    int index = representationURI.toString().indexOf(
        "/records/" + cloudId.getId() + "/representations/" + representationName + "/versions/" + VERSION);
    assertThat(index, not(-1));
  }

  @Test
  public void createRepresentationInGivenVersionFileKeyValue() throws CloudException, MCSException, IOException {
    String representationName = "StrangeRepresentationName";

    UISClient uisClient = new UISClient(LOCAL_TEST_UIS_URL, USER_NAME, USER_PASSWORD);
    CloudId cloudId = uisClient.getCloudId(PROVIDER_ID, "recordt1");

    String filename = "log4j.properties";
    String mediatype = MediaType.TEXT_PLAIN_VALUE;
    InputStream is = RecordServiceClientTestIT.class.getResourceAsStream("/" + filename);

    RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
    URI representationURI = mcsClient.createRepresentation(cloudId.getId(), representationName, PROVIDER_ID,
        VERSION, DATASET_ID, is, filename, mediatype);

    int index = representationURI.toString().indexOf(
        "/records/" + cloudId.getId() + "/representations/" + representationName + "/versions/");
    assertThat(index, not(-1));
  }


  @Test(expected = RepresentationNotExistsException.class)
  public void deleteRepresentation() throws CloudException, MCSException, IOException {
    String representationName = "StrangeRepresentationName";

    UISClient uisClient = new UISClient(REMOTE_TEST_UIS_URL, USER_NAME, USER_PASSWORD);
    CloudId cloudId = uisClient.createCloudId(PROVIDER_ID);

    String filename = "log4j.properties";
    String mediatype = MediaType.TEXT_PLAIN_VALUE;
    InputStream is = RecordServiceClientTestIT.class.getResourceAsStream("/" + filename);

    RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, ADMIN_NAME, ADMIN_PASSWORD);
    mcsClient.createRepresentation(cloudId.getId(), representationName, PROVIDER_ID,
        DATASET_ID, is, filename, mediatype);

    Representation representation1 = mcsClient.getRepresentation(cloudId.getId(), representationName);
    assertNotNull(representation1);

    mcsClient.deleteRepresentation(cloudId.getId(), representationName);
    Representation representation2 = mcsClient.getRepresentation(cloudId.getId(), representationName);
  }

  //    public void deleteRepresentation(String cloudId, String representationName)


  //http://localhost:8080/mcs/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA/representations
  //https://test.ecloud.psnc.pl/api/records/SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA/representations
  @Test
  public void getRepresentationsName() throws MCSException {
    String cloudId = "SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA";

    RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
    List<Representation> representations = mcsClient.getRepresentations(cloudId, "metadataRecord");
    assertThat(representations.size(), is(2));

    for (Representation representation : representations) {
      assertThat(representation.getCloudId(), is(cloudId));
    }
  }

  @Test
  public void getRepresentationVersion() throws MCSException {
    String cloudId = "SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA";
    String version = "";

    RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
    Representation representation = mcsClient.getRepresentation(cloudId, "metadataRecord", version);
  }

  @Test
  public void getRepresentationVersionKeyValue() throws MCSException {
    String cloudId = "SPBD7WGIBOP6IJSEHSJJL6BTQ7SSSTS2TA3MB6R6O2QTUREKU5DA";
    String version = "";
    String key = "";
    String value = "";

    RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
    Representation representation = mcsClient.getRepresentation(cloudId, "metadataRecord", version, key, value);
  }

  @Test
  public void deleteRepresentationVersion() throws CloudException, MCSException {
    String representationName = "StrangeRepresentationName";
    String version = "";

    UISClient uisClient = new UISClient(REMOTE_TEST_UIS_URL, USER_NAME, USER_PASSWORD);
    CloudId cloudId = uisClient.createCloudId(PROVIDER_ID);

    RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
    URI representationURI = mcsClient.createRepresentation(cloudId.getId(), representationName, PROVIDER_ID, DATASET_ID);

    mcsClient.deleteRepresentation(cloudId.getId(), representationName, version);
    Representation representation = mcsClient.getRepresentation(cloudId.getId(), representationName);

    int index = representationURI.toString().indexOf(
        "/records/" + cloudId.getId() + "/representations/" + representationName + "/versions/");
    assertThat(index, not(-1));
  }

/*
    public void deleteRepresentation(String cloudId, String representationName, String version, String key, String value)
 */

  @Test
  public void persistRepresentation() throws CloudException, MCSException {
    String representationName = "StrangeRepresentationName";
    String version = "";

    UISClient uisClient = new UISClient(REMOTE_TEST_UIS_URL, USER_NAME, USER_PASSWORD);
    CloudId cloudId = uisClient.createCloudId(PROVIDER_ID);

    RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
    URI representationURI = mcsClient.persistRepresentation(cloudId.getId(), representationName, PROVIDER_ID);

    Representation representation = mcsClient.getRepresentation(cloudId.getId(), representationName);

    int index = representationURI.toString().indexOf(
        "/records/" + cloudId.getId() + "/representations/" + representationName + "/versions/");
    assertThat(index, not(-1));
  }

  @Test
  public void getRepresentationsByRevision() throws MCSException {
    String cloudId = "<enter_cloud_id_here>";
    String representationName = "<enter_representation_name_here>";
    String revisionName = "<enter_revision_name_here>";
    String revisionProviderId = "<enter_revision_provider_id_here>";
    String revisionTimestamp = "<enter_revision_timestamp_here[YYYY-MM-ddThh:mm:ss.sss]>";

    RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
    List<Representation> representations = mcsClient.getRepresentationsByRevision(cloudId, representationName,
        new Revision(revisionName, revisionProviderId, DateHelper.parseISODate(revisionTimestamp)));

    assertNotNull(representations);
  }

  @Test
  public void getRepresentationsByRevisionRealData() throws MCSException {
    String cloudId = "222B5I4VPV3XN43PZMD2UHC6NPA6B2ZY7ZRPQV2UUVXRHFDALXEA";
    String representationName = "metadataRecord";
    String revisionName = "VALIDATION_EXTERNAL";
    String revisionProviderId = "metis_acceptance";
    String revisionTimestamp = "2019-09-26T16:30:04.972";

    RecordServiceClient mcsClient = new RecordServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
    List<Representation> representations = mcsClient.getRepresentationsByRevision(cloudId, representationName,
        new Revision(revisionName, revisionProviderId, DateHelper.parseISODate(revisionTimestamp)));

    assertNotNull(representations);
    assertTrue(representations.size() > 0);
    assertThat(representations.get(0).getCloudId(), is(cloudId));
  }

}
