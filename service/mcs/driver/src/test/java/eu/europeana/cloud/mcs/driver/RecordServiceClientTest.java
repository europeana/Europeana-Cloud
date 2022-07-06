package eu.europeana.cloud.mcs.driver;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.test.WiremockHelper;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.hamcrest.number.OrderingComparisons.greaterThan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;

public class RecordServiceClientTest {

    private static final String DATASET_ID = "31ad3e70-be5f-45bc-b60e-f102ff24fa88";
    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(8080));

    private final String baseUrl = "http://127.0.0.1:8080/mcs";

    /**
     * Should already exist in the system
     */
    private static final String CLOUD_ID = "W3KBLNZDKNQ";
    private static final String PROVIDER_ID = "Provider001";
    private static final String REPRESENTATION_NAME = "schema66";

    private static final String VERSION = "881c5c00-4259-11e4-9c35-00163eefc9c8";
    // -- //

    /**
     * Should not exist in the system
     */
    private static final String NON_EXISTING_REPRESENTATION_NAME = "NON_EXISTING_REPRESENTATION_NAME_12";

    private static final String username = "Olga";
    private static final String password = "Tokarczuk";

    // getRecord
    @Test
    public void shouldRetrieveRecord() throws MCSException {

        //
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/W3KBLNZDKNQ",
                200,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><record><cloudId>W3KBLNZDKNQ</cloudId><representations><allVersionsUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions</allVersionsUri><creationDate>2014-09-23T14:27:06.512+02:00</creationDate><dataProvider>Provider001</dataProvider><persistent>true</persistent><representationName>schema66</representationName><uri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/ee161f50-431c-11e4-8576-00163eefc9c8</uri><version>ee161f50-431c-11e4-8576-00163eefc9c8</version></representations><representations><allVersionsUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema33/versions</allVersionsUri><creationDate>2014-09-23T14:27:07.209+02:00</creationDate><dataProvider>Provider001</dataProvider><persistent>true</persistent><representationName>schema33</representationName><uri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema33/versions/ee9d50b0-431c-11e4-8576-00163eefc9c8</uri><version>ee9d50b0-431c-11e4-8576-00163eefc9c8</version></representations></record>");
        //

        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        Record record = instance.getRecord(CLOUD_ID);
        assertNotNull(record);
        assertEquals(CLOUD_ID, record.getCloudId());
    }

    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowRecordNotExistsForGetRecord() throws MCSException {
        String cloudId = "noSuchRecord";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        //
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/noSuchRecord",
                404,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>There is no record with provided global id: noSuchRecord</details><errorCode>RECORD_NOT_EXISTS</errorCode></errorInfo>");
        //

        instance.getRecord(cloudId);
    }

    @Test
    public void shouldDeleteRecord() throws MCSException {

        String cloudId = "231PJ0QGW6N";
        String representationName = "schema77";
        RecordServiceClient instance = new RecordServiceClient(baseUrl, username, password);

        //
        new WiremockHelper(wireMockRule).stubDelete(
                "/mcs/records/231PJ0QGW6N",
                204);

        // delete record
        instance.deleteRecord(cloudId);
        assertTrue(true);
    }

    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForDeleteRecordWhenNoRepresentations()
            throws MCSException {

        String cloudId = "25DG622J4VM";
        String representationName = "schema1";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        //
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/25DG622J4VM/representations/schema1/versions",
                404,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>REPRESENTATION_NOT_EXISTS</errorCode></errorInfo>");
        //

        // check that there are not representations for this record
        // we only check one representationName, because there is no method to
        // just get all representations
        List<Representation> representations = instance.getRepresentations(
                cloudId, representationName);
        assertEquals(0, representations.size());

        // delete record
        instance.deleteRecord(cloudId);
    }

    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRecordNotExistsForDeleteRecord() throws MCSException {

        String cloudId = "noSuchRecord";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        //
        new WiremockHelper(wireMockRule).stubDelete(
                "/mcs/records/noSuchRecord",
                405,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
        //
        instance.deleteRecord(cloudId);
    }


    // getRepresentations(cloudId)
    @Test
    public void shouldRetrieveRepresentations() throws MCSException {

        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        //
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/W3KBLNZDKNQ/representations",
                200,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><representations><representation><allVersionsUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions</allVersionsUri><cloudId>W3KBLNZDKNQ</cloudId><creationDate>2014-09-23T14:27:06.512+02:00</creationDate><dataProvider>Provider001</dataProvider><persistent>true</persistent><representationName>schema66</representationName><uri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/ee161f50-431c-11e4-8576-00163eefc9c8</uri><version>ee161f50-431c-11e4-8576-00163eefc9c8</version></representation><representation><allVersionsUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema33/versions</allVersionsUri><cloudId>W3KBLNZDKNQ</cloudId><creationDate>2014-09-23T14:27:07.209+02:00</creationDate><dataProvider>Provider001</dataProvider><persistent>true</persistent><representationName>schema33</representationName><uri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema33/versions/ee9d50b0-431c-11e4-8576-00163eefc9c8</uri><version>ee9d50b0-431c-11e4-8576-00163eefc9c8</version></representation></representations>");
        //

        List<Representation> representationList = instance
                .getRepresentations(CLOUD_ID);
        assertNotNull(representationList);
        // in this scenario we have 3 persistent representations, 2 in one
        // representation name and 1 in another, thus we want to get 2
        assertEquals(2, representationList.size());
        for (Representation representation : representationList) {
            assertEquals(CLOUD_ID, representation.getCloudId());
            assertTrue(representation.isPersistent());
        }
    }

    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowRecordNotExistsForGetRepresentations()
            throws MCSException {
        String cloudId = "noSuchRecord";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        //
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/noSuchRecord/representations",
                404,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>There is no record with provided global id: noSuchRecord</details><errorCode>RECORD_NOT_EXISTS</errorCode></errorInfo>");
        //

        instance.getRepresentations(cloudId);
    }


    // getRepresentations(cloudId, representationName)
    @Test
    public void shouldRetrieveLastPersistentRepresentationForRepresentationName()
            throws MCSException {
        // String cloudId = "J93T5R6615H";
        // String representationName = "schema1";
        // //the last persisent representation
        // String version = "acf7a040-9587-11e3-8f2f-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        //
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/W3KBLNZDKNQ/representations/schema66",
                200,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><representation><allVersionsUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions</allVersionsUri><cloudId>W3KBLNZDKNQ</cloudId><creationDate>2014-09-23T14:14:50.930+02:00</creationDate><dataProvider>Provider001</dataProvider><persistent>true</persistent><representationName>schema66</representationName><uri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/37a6e520-431b-11e4-8576-00163eefc9c8</uri><version>37a6e520-431b-11e4-8576-00163eefc9c8</version></representation>");
        //

        Representation representation = instance.getRepresentation(CLOUD_ID,
                REPRESENTATION_NAME);

        assertNotNull(representation);
        // assertEquals(cloudId, representation.getCloudId());
        // assertEquals(representationName,
        // representation.getRepresentationName());
        // assertEquals(version, representation.getVersion());
        assertTrue(representation.isPersistent());
    }

    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationForRepresentationNameWhenNoRepresentationName()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        // no representation for this representation name
        String representationName = "noSuchSchema";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        //
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/7MZWQJF8P84/representations/noSuchSchema",
                404,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>REPRESENTATION_NOT_EXISTS</errorCode></errorInfo>");
        //

        instance.getRepresentation(cloudId, representationName);
    }

    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationForRepresentationNameWhenNoPersistent()
            throws MCSException {
        String cloudId = "GWV0RHNSSGJ";
        // there are representations for this representation name, but none is
        // persistent
        String representationName = "schema1";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);
        //
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/GWV0RHNSSGJ/representations/schema1",
                404,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>REPRESENTATION_NOT_EXISTS</errorCode></errorInfo>");
        //

        instance.getRepresentation(cloudId, representationName);
    }

    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowRecordNotExistsForCreateRepresentation()
            throws MCSException {

        String cloudId = "noSuchRecord";
        String representationName = "schema_000001";
        String providerId = "Provider001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        //
        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/noSuchRecord/representations/schema_000001",
                404,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>There is no record with provided global id: noSuchRecord</details><errorCode>RECORD_NOT_EXISTS</errorCode></errorInfo>");
        //

        instance.createRepresentation(cloudId, representationName, providerId, DATASET_ID);
    }

    @Test
    public void shouldCreateNewSchemaWhenNotExists() throws MCSException {

        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        //
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/W3KBLNZDKNQ/representations/NON_EXISTING_REPRESENTATION_NAME_12/versions",
                404,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>REPRESENTATION_NOT_EXISTS</errorCode></errorInfo>");
        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/W3KBLNZDKNQ/representations/NON_EXISTING_REPRESENTATION_NAME_12",
                201,
                "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/NON_EXISTING_REPRESENTATION_NAME_12/versions/ed9f41a0-431c-11e4-8576-00163eefc9c8",
                null);
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/W3KBLNZDKNQ/representations/NON_EXISTING_REPRESENTATION_NAME_12/versions/ed9f41a0-431c-11e4-8576-00163eefc9c8",
                200,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><representation><allVersionsUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/NON_EXISTING_REPRESENTATION_NAME_12/versions</allVersionsUri><cloudId>W3KBLNZDKNQ</cloudId><creationDate>2014-09-23T14:27:05.238+02:00</creationDate><dataProvider>Provider001</dataProvider><persistent>false</persistent><representationName>NON_EXISTING_REPRESENTATION_NAME_12</representationName><uri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/NON_EXISTING_REPRESENTATION_NAME_12/versions/ed9f41a0-431c-11e4-8576-00163eefc9c8</uri><version>ed9f41a0-431c-11e4-8576-00163eefc9c8</version></representation>");
        //

        // ensure representation name does not exist
        boolean noRepresentationName = false;
        try {
            instance.getRepresentations(CLOUD_ID,
                    NON_EXISTING_REPRESENTATION_NAME);
        } catch (RepresentationNotExistsException ex) {
            noRepresentationName = true;
        }
        assertTrue(noRepresentationName);

        URI uri = instance.createRepresentation(CLOUD_ID,
                NON_EXISTING_REPRESENTATION_NAME, PROVIDER_ID, DATASET_ID);
        TestUtils.assertCorrectlyCreatedRepresentation(instance, uri,
                PROVIDER_ID, CLOUD_ID, NON_EXISTING_REPRESENTATION_NAME);
    }

    @Test(expected = ProviderNotExistsException.class)
    public void shouldThrowProviderNotExistsForCreateRepresentation()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        String representationName = "schema_000001";
        String providerId = "noSuchProvider";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);
        //
        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/7MZWQJF8P84/representations/schema_000001",
                404,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Provider noSuchProvider does not exist.</details><errorCode>PROVIDER_NOT_EXISTS</errorCode></errorInfo>");
        //

        instance.createRepresentation(cloudId, representationName, providerId, DATASET_ID);
    }

    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRepresentationNotExistsForDeleteRepresentationNameWhenNoRepresentationName()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "noSuchSchema";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        //
        new WiremockHelper(wireMockRule).stubDelete(
                "/mcs/records/J93T5R6615H/representations/noSuchSchema",
                405,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
        //

        instance.deleteRepresentation(cloudId, representationName);
    }

    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRepresentationNotExistsForDeleteRepresentationNameWhenNoRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        String representationName = "schema1";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);
        //
        new WiremockHelper(wireMockRule).stubDelete(
                "/mcs/records/noSuchRecord/representations/schema1",
                405,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
        //
        instance.deleteRepresentation(cloudId, representationName);
    }


    // getRepresentations(cloudId, representationName)
    @Test
    public void shouldRetrieveSchemaVersions()
            throws MCSException {

        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);
        //
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions",
                200,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><representations><representation><allVersionsUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions</allVersionsUri><cloudId>W3KBLNZDKNQ</cloudId><creationDate>2014-09-23T14:14:50.930+02:00</creationDate><dataProvider>Provider001</dataProvider><persistent>true</persistent><representationName>schema66</representationName><uri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/37a6e520-431b-11e4-8576-00163eefc9c8</uri><version>37a6e520-431b-11e4-8576-00163eefc9c8</version></representation><representation><allVersionsUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions</allVersionsUri><cloudId>W3KBLNZDKNQ</cloudId><creationDate>2014-09-23T14:14:45.566+02:00</creationDate><dataProvider>Provider001</dataProvider><persistent>false</persistent><representationName>schema66</representationName><uri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/34b91400-431b-11e4-8576-00163eefc9c8</uri><version>34b91400-431b-11e4-8576-00163eefc9c8</version></representation></representations>");
        //

        List<Representation> result = instance.getRepresentations(CLOUD_ID,
                REPRESENTATION_NAME);
        assertNotNull(result);
        assertThat(result.size(), greaterThan(1));
        for (Representation representation : result) {
            assertEquals(CLOUD_ID, representation.getCloudId());
            assertEquals(REPRESENTATION_NAME,
                    representation.getRepresentationName());
        }
    }

    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationNameVersionsWhenNoRepresentationName()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "noSuchSchema";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);
        //
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/J93T5R6615H/representations/noSuchSchema/versions",
                404,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>REPRESENTATION_NOT_EXISTS</errorCode></errorInfo>");
        //
        instance.getRepresentations(cloudId, representationName);
    }

    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationNameVersionsWhenNoRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        String representationName = "schema1";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        //
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/noSuchRecord/representations/schema1/versions",
                404,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>REPRESENTATION_NOT_EXISTS</errorCode></errorInfo>");
        //

        instance.getRepresentations(cloudId, representationName);
    }


    // getRepresentation(cloudId, representationName, version)
    @Test
    public void shouldRetrieveRepresentationVersion() throws MCSException {

        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        //
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/881c5c00-4259-11e4-9c35-00163eefc9c8",
                200,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><representation><allVersionsUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions</allVersionsUri><cloudId>W3KBLNZDKNQ</cloudId><creationDate>2014-09-23T13:52:23.474+02:00</creationDate><dataProvider>Provider001</dataProvider><files><contentLength>16</contentLength><contentUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/881c5c00-4259-11e4-9c35-00163eefc9c8/files/11ed22b2-89f7-4ba5-8967-65dc0b77bb9d</contentUri><date>2014-09-22T16:30:11.800+02:00</date><fileName>11ed22b2-89f7-4ba5-8967-65dc0b77bb9d</fileName><md5>0b083eb6ea615b11f86211c90fe733ae</md5><mimeType>text/plain</mimeType></files><files><contentLength>16</contentLength><contentUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/881c5c00-4259-11e4-9c35-00163eefc9c8/files/3345c8e4-bc57-4d41-8bd0-afeb8b221b98</contentUri><date>2014-09-22T16:30:11.065+02:00</date><fileName>3345c8e4-bc57-4d41-8bd0-afeb8b221b98</fileName><md5>819b4221a033c38d69bde0169d62720b</md5><mimeType>text/plain</mimeType></files><files><contentLength>16</contentLength><contentUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/881c5c00-4259-11e4-9c35-00163eefc9c8/files/4e79e8ae-1bcd-444b-a7cb-efacf2ab815e</contentUri><date>2014-09-22T16:30:11.964+02:00</date><fileName>4e79e8ae-1bcd-444b-a7cb-efacf2ab815e</fileName><md5>7fc41079b286e06f033991cf9e1a3cd9</md5><mimeType>text/plain</mimeType></files><files><contentLength>16</contentLength><contentUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/881c5c00-4259-11e4-9c35-00163eefc9c8/files/6480a4db-9f8f-424b-bbc3-7706198dd16a</contentUri><date>2014-09-22T16:30:12.493+02:00</date><fileName>6480a4db-9f8f-424b-bbc3-7706198dd16a</fileName><md5>a2eff66ac1e4c540eb182ee32551041e</md5><mimeType>text/plain</mimeType></files><files><contentLength>16</contentLength><contentUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/881c5c00-4259-11e4-9c35-00163eefc9c8/files/8896ecee-fa3a-44f4-ad7c-dc6ae3d78e0d</contentUri><date>2014-09-22T16:30:12.325+02:00</date><fileName>8896ecee-fa3a-44f4-ad7c-dc6ae3d78e0d</fileName><md5>b6cf0a0b31943991ee92a71c1e081185</md5><mimeType>text/plain</mimeType></files><files><contentLength>16</contentLength><contentUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/881c5c00-4259-11e4-9c35-00163eefc9c8/files/8927ce4c-5982-4e16-9cab-35fb40005a14</contentUri><date>2014-09-22T16:30:11.630+02:00</date><fileName>8927ce4c-5982-4e16-9cab-35fb40005a14</fileName><md5>c8a9994f45739425e5b6ad5f7f0394fa</md5><mimeType>text/plain</mimeType></files><files><contentLength>16</contentLength><contentUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/881c5c00-4259-11e4-9c35-00163eefc9c8/files/b43c1512-d05a-4302-aeed-1b0b811df2e1</contentUri><date>2014-09-22T16:30:11.464+02:00</date><fileName>b43c1512-d05a-4302-aeed-1b0b811df2e1</fileName><md5>b7aa921bb68bf0a1f2b2a952bfb0902d</md5><mimeType>text/plain</mimeType></files><files><contentLength>16</contentLength><contentUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/881c5c00-4259-11e4-9c35-00163eefc9c8/files/c5a33850-363e-419b-9804-e3697c5c81c6</contentUri><date>2014-09-22T16:30:12.129+02:00</date><fileName>c5a33850-363e-419b-9804-e3697c5c81c6</fileName><md5>4742b09e415f28826236b6b5638e30dd</md5><mimeType>text/plain</mimeType></files><files><contentLength>16</contentLength><contentUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/881c5c00-4259-11e4-9c35-00163eefc9c8/files/ddf05e89-d0d5-420c-8b96-4971801cf0a7</contentUri><date>2014-09-22T16:30:11.284+02:00</date><fileName>ddf05e89-d0d5-420c-8b96-4971801cf0a7</fileName><md5>cc3dedabc38bdafc5a5fd53b5485544f</md5><mimeType>text/plain</mimeType></files><files><contentLength>16</contentLength><contentUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/881c5c00-4259-11e4-9c35-00163eefc9c8/files/f5b0cd7f-f8ec-4834-8537-b7ff3171279b</contentUri><date>2014-09-22T16:30:12.653+02:00</date><fileName>f5b0cd7f-f8ec-4834-8537-b7ff3171279b</fileName><md5>3dff79dbbb0a78108d6b99657b10428d</md5><mimeType>text/plain</mimeType></files><persistent>true</persistent><representationName>schema66</representationName><uri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/881c5c00-4259-11e4-9c35-00163eefc9c8</uri><version>881c5c00-4259-11e4-9c35-00163eefc9c8</version></representation>");
        //

        Representation representation = instance.getRepresentation(CLOUD_ID,
                REPRESENTATION_NAME, VERSION);
        assertNotNull(representation);
        assertEquals(CLOUD_ID, representation.getCloudId());
        assertEquals(REPRESENTATION_NAME,
                representation.getRepresentationName());
        assertEquals(VERSION, representation.getVersion());
    }

    @Test
    public void shouldRetrieveLatestRepresentationVersion() throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        // this is the version of latest persistent version
        String versionCode = "88edb4d0-a2ef-11e3-89f5-1c6f653f6012";

        //
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/J93T5R6615H/representations/schema22/versions/88edb4d0-a2ef-11e3-89f5-1c6f653f6012",
                200,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><representation><allVersionsUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/J93T5R6615H/representations/schema22/versions</allVersionsUri><cloudId>J93T5R6615H</cloudId><creationDate>2014-09-23T13:52:23.474+02:00</creationDate><dataProvider>Provider001</dataProvider><files><contentLength>16</contentLength><contentUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/881c5c00-4259-11e4-9c35-00163eefc9c8/files/f5b0cd7f-f8ec-4834-8537-b7ff3171279b</contentUri><date>2014-09-22T16:30:12.653+02:00</date><fileName>f5b0cd7f-f8ec-4834-8537-b7ff3171279b</fileName><md5>3dff79dbbb0a78108d6b99657b10428d</md5><mimeType>text/plain</mimeType></files><persistent>true</persistent><representationName>schema22</representationName><uri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/881c5c00-4259-11e4-9c35-00163eefc9c8</uri><version>88edb4d0-a2ef-11e3-89f5-1c6f653f6012</version></representation>");
        //
        RecordServiceClient instance = new RecordServiceClient(baseUrl, username, password);

        Representation representationLatest = instance.getRepresentation(cloudId, representationName, versionCode);
        assertNotNull(representationLatest);
        assertEquals(cloudId, representationLatest.getCloudId());
        assertEquals(representationName,
                representationLatest.getRepresentationName());
        assertEquals(versionCode, representationLatest.getVersion());
    }

    @Test
    public void shouldTreatLatestPersistentVersionAsLatestCreated() throws MCSException {

        String fileType = "text/plain";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);
        FileServiceClient fileService = new FileServiceClient(baseUrl,
                username, password);

        //
        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/W3KBLNZDKNQ/representations/schema66",
                201,
                "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/ee161f50-431c-11e4-8576-00163eefc9c8",
                null);
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/ee161f50-431c-11e4-8576-00163eefc9c8",
                200,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><representation><allVersionsUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions</allVersionsUri><cloudId>W3KBLNZDKNQ</cloudId><creationDate>2014-09-23T14:27:06.017+02:00</creationDate><dataProvider>Provider001</dataProvider><persistent>false</persistent><representationName>schema66</representationName><uri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/ee161f50-431c-11e4-8576-00163eefc9c8</uri><version>ee161f50-431c-11e4-8576-00163eefc9c8</version></representation>");

        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/ee161f50-431c-11e4-8576-00163eefc9c8/persist",
                201,
                "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/ee161f50-431c-11e4-8576-00163eefc9c8",
                null);

        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/ee161f50-431c-11e4-8576-00163eefc9c8/files",
                201,
                "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/ee161f50-431c-11e4-8576-00163eefc9c8/files/7eba85ea-aab8-4a87-9fd3-781ff48be618",
                null);

        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/W3KBLNZDKNQ/representations/schema66",
                200,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><representation><allVersionsUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions</allVersionsUri><cloudId>W3KBLNZDKNQ</cloudId><creationDate>2014-09-23T14:27:06.512+02:00</creationDate><dataProvider>Provider001</dataProvider><persistent>true</persistent><representationName>schema66</representationName><uri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/ee161f50-431c-11e4-8576-00163eefc9c8</uri><version>ee161f50-431c-11e4-8576-00163eefc9c8</version></representation>");

        //
        // create representation A
        URI uriA = instance.createRepresentation(CLOUD_ID, REPRESENTATION_NAME, PROVIDER_ID, DATASET_ID);
        // create representation B
        URI uriB = instance.createRepresentation(CLOUD_ID, REPRESENTATION_NAME, PROVIDER_ID, DATASET_ID);
        // obtain version codes
        String versionA = TestUtils.obtainRepresentationFromURI(instance, uriA)
                .getVersion();
        String versionB = TestUtils.obtainRepresentationFromURI(instance, uriB)
                .getVersion();
        // add files
        fileService.uploadFile(CLOUD_ID, REPRESENTATION_NAME, versionA,
                new ByteArrayInputStream("fileA".getBytes()), fileType);
        fileService.uploadFile(CLOUD_ID, REPRESENTATION_NAME, versionB,
                new ByteArrayInputStream("fileB".getBytes()), fileType);
        // persist representation B
        instance.persistRepresentation(CLOUD_ID, REPRESENTATION_NAME, versionB);
        // persist representation A
        instance.persistRepresentation(CLOUD_ID, REPRESENTATION_NAME, versionA);
        // check what was obtained
        Representation representation = instance.getRepresentation(CLOUD_ID,
                REPRESENTATION_NAME);
        assertEquals(representation.getVersion(), versionB);
    }

    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationVersionWhenNoRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        String representationName = "schema22";
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        //
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/noSuchRecord/representations/schema22/versions/74cc8410-a2d9-11e3-8a55-1c6f653f6012",
                404,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>REPRESENTATION_NOT_EXISTS</errorCode></errorInfo>");
        //

        instance.getRepresentation(cloudId, representationName, version);
    }

    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationVersionWhenNoRepresentationName()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "noSuchSchema";
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        //
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/J93T5R6615H/representations/noSuchSchema/versions/74cc8410-a2d9-11e3-8a55-1c6f653f6012",
                404,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>REPRESENTATION_NOT_EXISTS</errorCode></errorInfo>");
        //

        instance.getRepresentation(cloudId, representationName, version);
    }

    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationVersionWhenNoSuchVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        // there is no such version, but the UUID is valid
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6013";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        //
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/J93T5R6615H/representations/schema22/versions/74cc8410-a2d9-11e3-8a55-1c6f653f6013",
                404,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>REPRESENTATION_NOT_EXISTS</errorCode></errorInfo>");
        //

        instance.getRepresentation(cloudId, representationName, version);
    }


    // deleteRepresentation(cloudId, representationName, version)
    @Test
    public void shouldDeleteRepresentationVersion() throws MCSException {
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        //
        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/W3KBLNZDKNQ/representations/schema66",
                "providerId=Provider001&dataSetId="+DATASET_ID,
                "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/f30eb490-431c-11e4-8576-00163eefc9c8",
                201,
                null);

        new WiremockHelper(wireMockRule).stubDelete(
                "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/f30eb490-431c-11e4-8576-00163eefc9c8",
                204);
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/f30eb490-431c-11e4-8576-00163eefc9c8",
                404,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>REPRESENTATION_NOT_EXISTS</errorCode></errorInfo>");
        //

        URI newReprURI = instance.createRepresentation(CLOUD_ID, REPRESENTATION_NAME, PROVIDER_ID, DATASET_ID);
        Representation repr = TestUtils.parseRepresentationFromUri(newReprURI);

        instance.deleteRepresentation(CLOUD_ID, REPRESENTATION_NAME,
                repr.getVersion());

        // try to get this version
        boolean noVersion = false;
        try {
            instance.getRepresentation(CLOUD_ID, REPRESENTATION_NAME,
                    repr.getVersion());
        } catch (RepresentationNotExistsException ex) {
            noVersion = true;
        }
        assertTrue(noVersion);
    }

    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRepresentationNotExistsForDeleteRepresentationVersionWhenNoRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        String representationName = "schema22";
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        new WiremockHelper(wireMockRule).stubDelete(
                "/mcs/records/noSuchRecord/representations/schema22/versions/74cc8410-a2d9-11e3-8a55-1c6f653f6012",
                405,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");

        instance.deleteRepresentation(cloudId, representationName, version);
    }

    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRepresentationNotExistsForDeleteRepresentationVersionWhenNoRepresentationName()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "noSuchSchema";
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        new WiremockHelper(wireMockRule).stubDelete(
                "/mcs/records/J93T5R6615H/representations/noSuchSchema/versions/74cc8410-a2d9-11e3-8a55-1c6f653f6012",
                405,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");

        instance.deleteRepresentation(cloudId, representationName, version);
    }

    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRepresentationNotExistsForDeleteRepresentationVersionWhenNoSuchVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        // there is no such version, but the UUID is valid
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6013";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        new WiremockHelper(wireMockRule).stubDelete(
                "/mcs/records/J93T5R6615H/representations/schema22/versions/74cc8410-a2d9-11e3-8a55-1c6f653f6013",
                405,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");

        instance.deleteRepresentation(cloudId, representationName, version);
    }

    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForDeleteRepresentationVersionWhenInvalidVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        // there is no such version and the UUID is invalid
        String version = "noSuchVersion";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        new WiremockHelper(wireMockRule).stubDelete(
                "/mcs/records/J93T5R6615H/representations/schema22/versions/noSuchVersion",
                405,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");

        instance.deleteRepresentation(cloudId, representationName, version);
    }

    @Test(expected = CannotModifyPersistentRepresentationException.class)
    public void shouldNotAllowToDeletePersistenRepresentation() throws MCSException {

        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/881c5c00-4259-11e4-9c35-00163eefc9c8/persist",
                405,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>CANNOT_MODIFY_PERSISTENT_REPRESENTATION</errorCode></errorInfo>");

        URI persistedReprURI = instance.persistRepresentation(CLOUD_ID,
                REPRESENTATION_NAME, VERSION);
        Representation persistedRepr = TestUtils.obtainRepresentationFromURI(
                instance, persistedReprURI);
        // check this representation is persistent
        assertTrue(persistedRepr.isPersistent());

        assertNotNull(persistedRepr);
        assertEquals(CLOUD_ID, persistedRepr.getCloudId());
        assertEquals(REPRESENTATION_NAME, persistedRepr.getRepresentationName());
        assertTrue(persistedRepr.isPersistent());

        // try to delete
        instance.deleteRepresentation(CLOUD_ID, REPRESENTATION_NAME, persistedRepr.getVersion());
    }

    // copyRepresentation

    // persistRepresentation
    @Test
    public void shouldPersistAfterAddingFiles() throws MCSException  {

        String representationName = "schema33";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);
        FileServiceClient fileService = new FileServiceClient(baseUrl,
                username, password);
        String fileContent = "The content of the file.";
        String fileType = "text/plain";

        //
        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/W3KBLNZDKNQ/representations/schema33",
                201,
                "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema33/versions/e542e7f0-431c-11e4-8576-00163eefc9c8",
                null);
        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/W3KBLNZDKNQ/representations/schema33/versions/e542e7f0-431c-11e4-8576-00163eefc9c8/files",
                201,
                "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema33/versions/e542e7f0-431c-11e4-8576-00163eefc9c8/files/efedee6f-e592-448a-b4d0-41eba6ec9d32",
                null);
        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/W3KBLNZDKNQ/representations/schema33/versions/e542e7f0-431c-11e4-8576-00163eefc9c8/persist",
                201,
                "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema33/versions/e542e7f0-431c-11e4-8576-00163eefc9c8",
                null);
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/W3KBLNZDKNQ/representations/schema33/versions/e542e7f0-431c-11e4-8576-00163eefc9c8",
                200,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><representation><allVersionsUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema33/versions</allVersionsUri><cloudId>W3KBLNZDKNQ</cloudId><creationDate>2014-09-23T14:26:51.645+02:00</creationDate><dataProvider>Provider001</dataProvider><files><contentLength>24</contentLength><contentUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema33/versions/e542e7f0-431c-11e4-8576-00163eefc9c8/files/efedee6f-e592-448a-b4d0-41eba6ec9d32</contentUri><date>2014-09-23T14:26:51.416+02:00</date><fileName>efedee6f-e592-448a-b4d0-41eba6ec9d32</fileName><md5>fad216b328837cadf9f7ae0ba54a8340</md5><mimeType>text/plain</mimeType></files><persistent>true</persistent><representationName>schema33</representationName><uri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema33/versions/e542e7f0-431c-11e4-8576-00163eefc9c8</uri><version>e542e7f0-431c-11e4-8576-00163eefc9c8</version></representation>");
        //
        // create representation
        URI uriCreated = instance.createRepresentation(CLOUD_ID, representationName, PROVIDER_ID, DATASET_ID);
        Representation coordinates = TestUtils
                .parseRepresentationFromUri(uriCreated);

        // add files
        InputStream data = new ByteArrayInputStream(fileContent.getBytes());
        fileService.uploadFile(CLOUD_ID, representationName, coordinates.getVersion(), data, fileType);

        // persist representation
        URI uriPersisted = instance.persistRepresentation(CLOUD_ID,
                representationName, coordinates.getVersion());

        assertNotNull(uriPersisted);
        Representation persistedRepresentation = TestUtils
                .obtainRepresentationFromURI(instance, uriPersisted);
        assertEquals(PROVIDER_ID, persistedRepresentation.getDataProvider());
        assertEquals(representationName,
                persistedRepresentation.getRepresentationName());
        assertEquals(CLOUD_ID, persistedRepresentation.getCloudId());
        assertEquals(coordinates.getVersion(),
                persistedRepresentation.getVersion());
        assertTrue(persistedRepresentation.isPersistent());
    }

    @Test(expected = CannotPersistEmptyRepresentationException.class)
    public void shouldNotPersistEmptyRepresentation() throws MCSException {

        String representationName = "schema33";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/W3KBLNZDKNQ/representations/schema33",
                201,
                "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema33/versions/e787f5f0-431c-11e4-8576-00163eefc9c8",
                null);

        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/W3KBLNZDKNQ/representations/schema33/versions/e787f5f0-431c-11e4-8576-00163eefc9c8/persist",
                405,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>CANNOT_PERSIST_EMPTY_REPRESENTATION</errorCode></errorInfo>");

        // create new representation version
        URI uri = instance.createRepresentation(CLOUD_ID, representationName, PROVIDER_ID, DATASET_ID);
        // obtain the version
        String version = TestUtils.parseRepresentationFromUri(uri).getVersion();
        // try to persist
        instance.persistRepresentation(CLOUD_ID, representationName, version);
    }

    @Test(expected = CannotModifyPersistentRepresentationException.class)
    public void shouldNotPersistRepresentationAgain() throws MCSException {

        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/W3KBLNZDKNQ/representations/schema66",
                200,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><representation><allVersionsUri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions</allVersionsUri><cloudId>W3KBLNZDKNQ</cloudId><creationDate>2014-09-23T14:27:06.512+02:00</creationDate><dataProvider>Provider001</dataProvider><persistent>true</persistent><representationName>schema66</representationName><uri>http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/ee161f50-431c-11e4-8576-00163eefc9c8</uri><version>ee161f50-431c-11e4-8576-00163eefc9c8</version></representation>");

        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/ee161f50-431c-11e4-8576-00163eefc9c8/persist",
                405,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>CANNOT_MODIFY_PERSISTENT_REPRESENTATION</errorCode></errorInfo>");

        // ensure this version is persistent
        Representation representation = instance.getRepresentation(CLOUD_ID, REPRESENTATION_NAME);
        assertTrue(representation.isPersistent());

        // try to persist
        instance.persistRepresentation(CLOUD_ID, REPRESENTATION_NAME, representation.getVersion());
    }

    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRepresentationNotExistsExceptionForPersistRepresentationWhenNoRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        String representationName = "schema33";
        String version = "fece3cb0-a5fb-11e3-b4a7-50e549e85271";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/noSuchRecord/representations/schema33/versions/fece3cb0-a5fb-11e3-b4a7-50e549e85271/persist",
                405,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
        instance.persistRepresentation(cloudId, representationName, version);
    }

    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRepresentationNotExistsExceptionForPersistRepresentationWhenNoRepresentationName()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "noSuchSchema";
        String version = "fece3cb0-a5fb-11e3-b4a7-50e549e85271";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        //
        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/J93T5R6615H/representations/noSuchSchema/versions/fece3cb0-a5fb-11e3-b4a7-50e549e85271/persist",
                405,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
        //

        instance.persistRepresentation(cloudId, representationName, version);
    }

    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRepresentationNotExistsExceptionForPersistRepresentationWhenNoSuchVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema33";
        String version = "fece3cb0-a5fb-11e3-b4a7-50e549e85204";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        //
        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/J93T5R6615H/representations/schema33/versions/fece3cb0-a5fb-11e3-b4a7-50e549e85204/persist",
                405,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
        //

        instance.persistRepresentation(cloudId, representationName, version);
    }

    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForPersistRepresentationVersionWhenInvalidVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema33";
        String version = "noSuchVersion";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        //
        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/J93T5R6615H/representations/schema33/versions/noSuchVersion/persist",
                405,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
        //

        instance.persistRepresentation(cloudId, representationName, version);
    }

    @Test
    public void shouldCreateNewRepresentationAndUploadAFile() throws IOException, MCSException {
        RecordServiceClient client = new RecordServiceClient("http://localhost:8080/mcs", "admin", "admin");
        InputStream stream = new ByteArrayInputStream("example File Content".getBytes(StandardCharsets.UTF_8));
        //
        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/FGDNTHPJQAUTEIGAHOALM2PMFSDRD726U5LNGMPYZZ34ZNVT5YGA/representations/sampleRepresentationName9/files",
                201,
                "http://localhost:8080/mcs/records/FGDNTHPJQAUTEIGAHOALM2PMFSDRD726U5LNGMPYZZ34ZNVT5YGA/representations/sampleRepresentationName9/versions/7a1ca2f0-5958-11e6-8345-90e6ba2d09ef/files/fileName",
                null);
        //
        client.createRepresentation("FGDNTHPJQAUTEIGAHOALM2PMFSDRD726U5LNGMPYZZ34ZNVT5YGA", "sampleRepresentationName9",
                "sampleProvider", DATASET_ID, stream, "fileName", "mediaType");
        assertTrue(true);
    }

    @Test
    public void shouldCreateNewRepresentationAndUploadAFile_1() throws IOException, MCSException {
        RecordServiceClient client = new RecordServiceClient("http://localhost:8080/mcs", "admin", "admin");
        InputStream stream = new ByteArrayInputStream("example File Content".getBytes(StandardCharsets.UTF_8));

        //
        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/FGDNTHPJQAUTEIGAHOALM2PMFSDRD726U5LNGMPYZZ34ZNVT5YGA/representations/sampleRepresentationName9/files",
                201,
                "http://localhost:8080/mcs/records/FGDNTHPJQAUTEIGAHOALM2PMFSDRD726U5LNGMPYZZ34ZNVT5YGA/representations/sampleRepresentationName9/versions/7a1ca2f0-5958-11e6-8345-90e6ba2d09ef/files/fileName",
                null);
        //
        client.createRepresentation("FGDNTHPJQAUTEIGAHOALM2PMFSDRD726U5LNGMPYZZ34ZNVT5YGA",
                "sampleRepresentationName9", "sampleProvider", DATASET_ID, stream, "mediaType");

        assertTrue(true);
    }

    @Test
    public void shouldRetrieveRepresentationRevision() throws MCSException {

        RecordServiceClient instance = new RecordServiceClient("http://localhost:8080/mcs", "admin", "admin");
        //
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/Z6DX3RWCEFUUSGRUWP6QZWRIZKY7HI5Y7H4UD3OQVB3SRPAUVZHA/representations/REPRESENTATION1/revisions/Revision_2?revisionProviderId=Revision_Provider&revisionTimestamp=2018-08-28T07%3A13%3A34.658Z",
                200,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><representations><representation><allVersionsUri>http://localhost:8080/mcs/records/2FIVVAQ5NC6WVNNPK7BKK2X3PB6PNDLMIGQYFGU3NQPWQ6DYSK2A/representations/REPRESENTATION1/versions</allVersionsUri><cloudId>2FIVVAQ5NC6WVNNPK7BKK2X3PB6PNDLMIGQYFGU3NQPWQ6DYSK2A</cloudId><creationDate>2019-09-09T12:53:29.238+02:00</creationDate><dataProvider>metis_test5</dataProvider><files><contentLength>2442</contentLength><contentUri>http://localhost:8080/mcs/records/2FIVVAQ5NC6WVNNPK7BKK2X3PB6PNDLMIGQYFGU3NQPWQ6DYSK2A/representations/REPRESENTATION1/versions/68b4cc30-aa8d-11e8-8289-1c6f653f9042/files/ba434eac-90cf-452f-891b-0cd8065341f4</contentUri><date>2019-09-09T12:53:29.232+02:00</date><fileName>ba434eac-90cf-452f-891b-0cd8065341f4</fileName><fileStorage>DATA_BASE</fileStorage><md5>bad9394e7c3ba724493ddc0677225d19</md5><mimeType>text/plain</mimeType></files><persistent>true</persistent><representationName>REPRESENTATION1</representationName><revisions><revisionName>revisionName</revisionName><revisionProviderId>metis_test</revisionProviderId><creationTimeStamp>2019-01-01T00:00:00.001Z</creationTimeStamp><published>false</published><acceptance>false</acceptance><deleted>false</deleted></revisions><uri>http://localhost:8080/mcs/records/2FIVVAQ5NC6WVNNPK7BKK2X3PB6PNDLMIGQYFGU3NQPWQ6DYSK2A/representations/REPRESENTATION1/versions/68b4cc30-aa8d-11e8-8289-1c6f653f9042</uri><version>68b4cc30-aa8d-11e8-8289-1c6f653f9042</version></representation></representations>");
        //

        // retrieve representation by revision
        List<Representation> representations = instance.getRepresentationsByRevision(
                "Z6DX3RWCEFUUSGRUWP6QZWRIZKY7HI5Y7H4UD3OQVB3SRPAUVZHA",
                "REPRESENTATION1",
                new Revision("Revision_2", "Revision_Provider", DateHelper.parseISODate("2018-08-28T07:13:34.658Z"))
        );
        assertNotNull(representations);
        assertEquals(1, representations.size());
        assertEquals("REPRESENTATION1",
                representations.get(0).getRepresentationName());
        assertEquals("68b4cc30-aa8d-11e8-8289-1c6f653f9042", representations.get(0).getVersion());
    }

    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExists() throws MCSException {

        RecordServiceClient instance = new RecordServiceClient("http://localhost:8080/mcs", "admin", "admin");
        //
        new WiremockHelper(wireMockRule).stubGet(
                "/mcs/records/Z6DX3RWCEFUUSGRUWP6QZWRIZKY7HI5Y7H4UD3OQVB3SRPAUVZHA/representations/REPRESENTATION2/revisions/Revision_2?revisionProviderId=Revision_Provider&revisionTimestamp=2018-08-28T07%3A13%3A34.658Z",
                404,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>No representation was found</details><errorCode>REPRESENTATION_NOT_EXISTS</errorCode></errorInfo>");
        //
        instance.getRepresentationsByRevision(
                "Z6DX3RWCEFUUSGRUWP6QZWRIZKY7HI5Y7H4UD3OQVB3SRPAUVZHA",
                "REPRESENTATION2",
                new Revision("Revision_2", "Revision_Provider", DateHelper.parseISODate("2018-08-28T07:13:34.658Z"))
        );

    }
}
