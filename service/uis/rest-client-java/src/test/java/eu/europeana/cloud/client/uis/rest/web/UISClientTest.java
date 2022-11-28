package eu.europeana.cloud.client.uis.rest.web;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.test.WiremockHelper;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests the UISClient, using Wiremock.
 * <p>
 * The rest API should be running in the URL defined below {@link UISClientTest#BASE_URL}
 * <p>
 * emmanouil.koufakis@theeuropeanlibrary.org
 */
public class UISClientTest {

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(8080));

  /**
   * Needed to record the tests.
   */
  private static final String BASE_URL_LOCALHOST = "http://localhost:8080/uis";
  private static final String BASE_URL = BASE_URL_LOCALHOST;

  private static final String PROVIDER_ID = "PROVIDER_1";

  private static final String RECORD_ID = "TEST_RECORD_1";

  private static final String username = "Cristiano";
  private static final String password = "Ronaldo";

  /**
   * Those provider ids must be unique and non-existing when the test is run
   */
  private static final String createMappingTest_PROVIDER_ID = "createMappingTest_PROVIDERID";
  private static final String duplicateProviderRecordTest_PROVIDER_ID = "duplicateProviderRecordTest_PROVIDERID";
  private static final String createAndRetrieveProviderTest_PROVIDER_ID = "createAndRetrieveProviderTest_PROVIDERID";
  private static final String createAndRetrieveRecordTest_PROVIDER_ID = "createAndRetrieveRecordTest_PROVIDERID";
  private static final String createCloudIdandRetrieveCloudIdTest_PROVIDER_ID = "createCloudIdandRetrieveCloudIdTest_PROVIDERID";
  private static final String updateProviderTest_PROVIDER_ID = "updateProviderTest_PROVIDERID";

  /**
   * Tests for:
   * <p>
   * Create, Retrieve provider.
   */
  @Test
  public final void createAndRetrieveProviderTest() throws Exception {

    UISClient uisClient = new UISClient(BASE_URL, username, password);

    new WiremockHelper(wireMockRule).stubPost(
        "/uis/data-providers?providerId=createAndRetrieveProviderTest_PROVIDERID",
        201,
        Map.of("Location",
            "http://localhost:8080/ecloud-service-uis-rest/data-providers/createAndRetrieveProviderTest_PROVIDERID"),
        null);

    new WiremockHelper(wireMockRule).stubGet("/uis/data-providers/createAndRetrieveProviderTest_PROVIDERID",
        200,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><dataProvider><id>createAndRetrieveProviderTest_PROVIDER_ID_12</id><partitionKey>-926013828</partitionKey><properties><contactPerson>person</contactPerson><digitalLibraryURL>url</digitalLibraryURL><digitalLibraryWebsite>url</digitalLibraryWebsite><officialAddress>Address</officialAddress><organisationName>Name</organisationName><organisationWebsite>website</organisationWebsite><organisationWebsiteURL>url</organisationWebsiteURL><remarks>remarks</remarks></properties></dataProvider>");

    // Create some test properties and store them in the cloud
    DataProviderProperties providerProperties = new DataProviderProperties("Name", "Address", "website",
        "url", "url", "url", "person", "remarks");
    uisClient.createProvider(createAndRetrieveProviderTest_PROVIDER_ID, providerProperties);

    // Get back the properties and make sure they are the same
    DataProvider provider = uisClient.getDataProvider(createAndRetrieveProviderTest_PROVIDER_ID);
    assertNotNull(provider);
    assertNotNull(provider.getProperties());
    assertEquals(provider.getProperties(), providerProperties);
  }

  /**
   * 1) Creates some test properties and stores them in the cloud => 2) Makes a call to Update the provider with new properties =>
   * 3) Retrieves the properties and makes sure the updated properties come back.
   */
  @Test
  public final void updateProviderTest() throws CloudException {

    UISClient uisClient = new UISClient(BASE_URL, username, password);

    new WiremockHelper(wireMockRule).stubPost(
        "/uis/data-providers?providerId=updateProviderTest_PROVIDERID",
        201,
        "http://localhost:8080/ecloud-service-uis-rest/data-providers/updateProviderTest_PROVIDERID",
        null);

    new WiremockHelper(wireMockRule).stubPut(
        "/uis/data-providers/updateProviderTest_PROVIDERID",
        204
    );

    new WiremockHelper(wireMockRule).stubGet(
        "/uis/data-providers/updateProviderTest_PROVIDERID",
        200,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><dataProvider><id>updateProviderTest_PROVIDERID</id><partitionKey>103212895</partitionKey><properties><contactPerson>person2</contactPerson><digitalLibraryURL>url2</digitalLibraryURL><digitalLibraryWebsite>url2</digitalLibraryWebsite><officialAddress>Address2</officialAddress><organisationName>Name2</organisationName><organisationWebsite>website2</organisationWebsite><organisationWebsiteURL>url2</organisationWebsiteURL><remarks>remarks2</remarks></properties></dataProvider>");

    // Create some test properties and store them in the cloud
    DataProviderProperties providerProperties = new DataProviderProperties("Name", "Address", "website",
        "url", "url", "url", "person", "remarks");
    uisClient.createProvider(updateProviderTest_PROVIDER_ID, providerProperties);

    // Make a call to Update the provider with new properties
    DataProviderProperties providerPropertiesUpdated = new DataProviderProperties("Name2", "Address2", "website2",
        "url2", "url2", "url2", "person2", "remarks2");
    uisClient.updateProvider(updateProviderTest_PROVIDER_ID, providerPropertiesUpdated);

    // Retrieve the properties and make sure you get the updated properties back
    DataProvider providerUpdated = uisClient.getDataProvider(updateProviderTest_PROVIDER_ID);
    assertNotNull(providerUpdated);
    assertNotNull(providerUpdated.getProperties());
    assertEquals(providerUpdated.getProperties(), providerPropertiesUpdated);
  }

  @Test
  public final void duplicateProviderRecordTest() throws Exception {

    UISClient uisClient = new UISClient(BASE_URL, username, password);

    new WiremockHelper(wireMockRule).stubPost(
        "/uis/data-providers?providerId=duplicateProviderRecordTest_PROVIDERID",
        201,
        "http://localhost:8080/ecloud-service-uis-rest/data-providers/duplicateProviderRecordTest_PROVIDER_ID_12",
        null);

    new WiremockHelper(wireMockRule).stubPost(
        "/uis/cloudIds?providerId=duplicateProviderRecordTest_PROVIDERID&recordId=TEST_RECORD_1",
        200,
        "http://localhost:8080/ecloud-service-uis-rest/data-providers/duplicateProviderRecordTest_PROVIDER_ID_12",
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><cloudId><id>KH5C38JBR3X</id><localId><providerId>duplicateProviderRecordTest_PROVIDERID</providerId><recordId>TEST_RECORD_1</recordId></localId></cloudId>");

    // Create a provider with some random properties
    DataProviderProperties providerProperties = new DataProviderProperties("Name", "Address", "website",
        "url", "url", "url", "person", "remarks");
    uisClient.createProvider(duplicateProviderRecordTest_PROVIDER_ID, providerProperties);

    // try to insert the same (PROVIDER_ID + RECORD_ID) twice
    uisClient.createCloudId(duplicateProviderRecordTest_PROVIDER_ID, RECORD_ID);
    try {
      uisClient.createCloudId(duplicateProviderRecordTest_PROVIDER_ID, RECORD_ID);
    } catch (Exception e) {
      assertTrue(e instanceof CloudException);
    }
  }

  @Test
  public final void createMappingTest() throws Exception {

    UISClient uisClient = new UISClient(BASE_URL, username, password);

    new WiremockHelper(wireMockRule).stubPost(
        "/uis/data-providers?providerId=createMappingTest_PROVIDERID",
        201,
        "http://localhost:8080/ecloud-service-uis-rest/data-providers/createMappingTest_PROVIDERID",
        null);

    new WiremockHelper(wireMockRule).stubPost(
        "/uis/cloudIds?providerId=createMappingTest_PROVIDERID",
        200,
        "http://localhost:8080/ecloud-service-uis-rest/data-providers/createMappingTest_PROVIDERID",
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><cloudId><id>7J9BZXFWTMG</id><localId><providerId>createMappingTest_PROVIDERID</providerId><recordId>2QTW98PT1PP</recordId></localId></cloudId>");

    new WiremockHelper(wireMockRule).stubGet(
        "/uis/cloudIds/7J9BZXFWTMG",
        200,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><resultSlice><results xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"cloudId\"><id>7J9BZXFWTMG</id><localId><providerId>createMappingTest_PROVIDERID</providerId><recordId>2QTW98PT1PP</recordId></localId></results></resultSlice>");

    // Create a provider with some random properties
    DataProviderProperties providerProperties = new DataProviderProperties("Name", "Address", "website",
        "url", "url", "url", "person", "remarks");
    uisClient.createProvider(createMappingTest_PROVIDER_ID, providerProperties);

    // create a mapping
    final CloudId cloudId = uisClient.createCloudId(createMappingTest_PROVIDER_ID);
    final LocalId localId = cloudId.getLocalId(); // local Id was created automatically
    assertNotNull(localId);

    // lets test that we can get some results back using the previously created CloudId
    ResultSlice<CloudId> resultSliceWithResults = uisClient.getRecordId(cloudId.getId());
    assertFalse(resultSliceWithResults.getResults().isEmpty());
  }

  @Test
  public final void createAndRetrieveRecordTest() throws Exception {

    UISClient uisClient = new UISClient(BASE_URL, username, password);

    new WiremockHelper(wireMockRule).stubPost(
        "/uis/data-providers?providerId=createAndRetrieveRecordTest_PROVIDERID",
        201,
        Map.of("Location", "http://localhost:8080/ecloud-service-uis-rest/data-providers/createAndRetrieveRecordTest_PROVIDERID"),
        null);

    new WiremockHelper(wireMockRule).stubPost(
        "/uis/cloudIds?providerId=createAndRetrieveRecordTest_PROVIDERID&recordId=TEST_RECORD_1",
        200,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><cloudId><id>255S7VQKHVM</id><localId><providerId>createAndRetrieveRecordTest_PROVIDER_ID_12</providerId><recordId>TEST_RECORD_1</recordId></localId></cloudId>");

    new WiremockHelper(wireMockRule).stubGet(
        "/uis/cloudIds/255S7VQKHVM",
        200,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><resultSlice><results xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"cloudId\"><id>255S7VQKHVM</id><localId><providerId>createAndRetrieveRecordTest_PROVIDER_ID_12</providerId><recordId>TEST_RECORD_1</recordId></localId></results></resultSlice>");

    // Create a provider with some random properties
    DataProviderProperties providerProperties = new DataProviderProperties("Name", "Address", "website",
        "url", "url", "url", "person", "remarks");
    uisClient.createProvider(createAndRetrieveRecordTest_PROVIDER_ID, providerProperties);

    // create a record
    final CloudId cloudIdIhave = uisClient.createCloudId(createAndRetrieveRecordTest_PROVIDER_ID, RECORD_ID);
    ResultSlice<CloudId> resultsSlice = uisClient.getRecordId(cloudIdIhave.getId());

    // get back the record
    CloudId cloudIdIGotBack = resultsSlice.getResults().iterator().next();
    assertEquals(cloudIdIhave, cloudIdIGotBack);
  }

  @Test
  public final void createCloudIdandRetrieveCloudIdTest() throws Exception {

    UISClient uisClient = new UISClient(BASE_URL, username, password);

    new WiremockHelper(wireMockRule).stubPost(
        "/uis/data-providers?providerId=createCloudIdandRetrieveCloudIdTest_PROVIDERID",
        201,
        Map.of("Location",
            "http://localhost:8080/ecloud-service-uis-rest/data-providers/createCloudIdandRetrieveCloudIdTest_PROVIDERID"),
        null);

    new WiremockHelper(wireMockRule).stubPost(
        "/uis/cloudIds?providerId=createCloudIdandRetrieveCloudIdTest_PROVIDERID&recordId=TEST_RECORD_1",
        200,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><cloudId><id>SP46XMN47N2</id><localId><providerId>createCloudIdandRetrieveCloudIdTest_PROVIDERID</providerId><recordId>TEST_RECORD_1</recordId></localId></cloudId>");

    new WiremockHelper(wireMockRule).stubGet(
        "/uis/cloudIds/SP46XMN47N2",
        200,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><resultSlice><results xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"cloudId\"><id>SP46XMN47N2</id><localId><providerId>createCloudIdandRetrieveCloudIdTest_PROVIDERID</providerId><recordId>TEST_RECORD_1</recordId></localId></results></resultSlice>");

    // Create a provider with some random properties
    DataProviderProperties providerProperties = new DataProviderProperties("Name", "Address", "website",
        "url", "url", "url", "person", "remarks");
    uisClient.createProvider(createCloudIdandRetrieveCloudIdTest_PROVIDER_ID, providerProperties);

    final CloudId cloudIdIhave = uisClient.createCloudId(createCloudIdandRetrieveCloudIdTest_PROVIDER_ID, RECORD_ID);
    ResultSlice<CloudId> resultsSlice = uisClient.getRecordId(cloudIdIhave.getId());

    CloudId cloudIdIGotBack = resultsSlice.getResults().iterator().next();
    assertEquals(cloudIdIhave, cloudIdIGotBack);
  }
}
