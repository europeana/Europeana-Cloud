package eu.europeana.cloud.mcs.driver;

import co.freeside.betamax.Betamax;
import co.freeside.betamax.Recorder;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import org.junit.Rule;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.number.OrderingComparisons.greaterThan;
import static org.junit.Assert.assertThat;
import org.junit.Ignore;
import org.junit.Test;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import static org.junit.Assert.assertNotEquals;

public class RecordServiceClientTest {

    @Rule
    public Recorder recorder = new Recorder();

    //TODO clean
    //this is only needed for recording tests
    private final String baseUrl = "http://localhost:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/";

    //getRecord
    @Betamax(tape = "records_shouldRetrieveRecord")
    @Test
    public void shouldRetrieveRecord()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        Record record = instance.getRecord(cloudId);
        assertNotNull(record);
        assertEquals(cloudId, record.getCloudId());
    }

    @Betamax(tape = "records_shouldThrowRecordNotExistsForGetRecord")
    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowRecordNotExistsForGetRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRecord(cloudId);
    }

    @Betamax(tape = "records_shouldThrowInternalServerErrorForGetRecord")
    @Test(expected = DriverException.class)
    public void shouldThrowInternalServerErrorForGetRecord()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRecord(cloudId);
    }

    //deleteRecord
    //this test could be better with Betamax 2.0 ability to hold state (not yet in main maven repository)
    @Betamax(tape = "records_shouldDeleteRecord")
    @Test
    public void shouldDeleteRecord()
            throws MCSException {

        String cloudId = "1DZ6HTS415W";
        String representationName = "schema77";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        //delete record
        instance.deleteRecord(cloudId);

        //check that there are not representations for this record
        //we only check one representationName, because there is no method to just get all representations
        List<Representation> representations = instance.getRepresentations(cloudId, representationName);
        assertEquals(representations.size(), 0);

    }

    @Betamax(tape = "records_shouldNotComplainAboutDeletingRecordWithNoRepresentations")
    @Test()
    public void shouldNotComplainAboutDeletingRecordWithNoRepresentations()
            throws MCSException {

        String cloudId = "1DZ6HTS415W";
        String representationName = "schema77";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        //check that there are not representations for this record
        //we only check one representationName, because there is no method to just get all representations
        List<Representation> representations = instance.getRepresentations(cloudId, representationName);
        assertEquals(representations.size(), 0);

        //delete record
        instance.deleteRecord(cloudId);
    }

    @Betamax(tape = "records_shouldThrowRecordNotExistsForDeleteRecord")
    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowRecordNotExistsForDeleteRecord()
            throws MCSException {

        String cloudId = "noSuchRecord";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRecord(cloudId);
    }

    @Betamax(tape = "records_shouldThrowInternalServerErrorForDeleteRecord")
    @Test(expected = DriverException.class)
    public void shouldThrowInternalServerErrorForDeleteRecord()
            throws MCSException {
        String cloudId = "1DZ6HTS415W";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRecord(cloudId);
    }

    //getRepresentations(cloudId)
    @Betamax(tape = "records_shouldRetrieveRepresentations")
    @Test
    public void shouldRetrieveRepresentations()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        List<Representation> representationList = instance.getRepresentations(cloudId);
        assertNotNull(representationList);
        //in this scenario we have 3 persistent representations, 2 in one representation name and 1 in another, thus we want to get 2
        assertEquals(representationList.size(), 2);
        for (Representation representation : representationList) {
            assertEquals(cloudId, representation.getCloudId());
            assertTrue(representation.isPersistent());
        }
    }

    @Betamax(tape = "records_shouldThrowRecordNotExistsForGetRepresentations")
    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowRecordNotExistsForGetRepresentations()
            throws MCSException {
        String cloudId = "noSuchRecord";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentations(cloudId);
    }

    @Betamax(tape = "records_shouldThrowInternalServerErrorForGetRepresentations")
    @Test(expected = DriverException.class)
    public void shouldThrowInternalServerErrorForGetRepresentations()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentations(cloudId);
    }

    //getRepresentations(cloudId, representationName)
    @Betamax(tape = "records_shouldRetrieveLastPersistentRepresentationForRepresentationName")
    @Test
    public void shouldRetrieveLastPersistentRepresentationForRepresentationName()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema1";
        //the last persisent representation
        String version = "acf7a040-9587-11e3-8f2f-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        Representation representation = instance.getRepresentation(cloudId, representationName);

        assertNotNull(representation);
        assertEquals(cloudId, representation.getCloudId());
        assertEquals(representationName, representation.getRepresentationName());
        assertEquals(version, representation.getVersion());
        assertTrue(representation.isPersistent());
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForGetRepresentationForRepresentationNameWhenNoRepresentationName")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationForRepresentationNameWhenNoRepresentationName()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        //no representation for this representation name
        String representationName = "noSuchSchema";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentation(cloudId, representationName);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForGetRepresentationForRepresentationNameWhenNoPersistent")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationForRepresentationNameWhenNoPersistent()
            throws MCSException {
        String cloudId = "GWV0RHNSSGJ";
        //there are representations for this representation name, but none is persistent
        String representationName = "schema1";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentation(cloudId, representationName);
    }

    @Betamax(tape = "records_shouldThrowInternalServerErrorForGetRepresentationForRepresentationName")
    @Test(expected = DriverException.class)
    public void shouldThrowInternalServerErrorForGetRepresentationForRepresentationName()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema1";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentation(cloudId, representationName);
    }

    //create representation
    @Betamax(tape = "records_shouldCreateRepresentation")
    @Test
    public void shouldCreateRepresentation()
            throws MCSException {

        String cloudId = "1DZ6HTS415W";
        String representationName = "schema77";
        String providerId = "Provider001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        URI uri = instance.createRepresentation(cloudId, representationName, providerId);
        TestUtils.assertCorrectlyCreatedRepresentation(instance, uri, providerId, cloudId, representationName);

    }

    @Betamax(tape = "records_shouldThrowRecordNotExistsForCreateRepresentation")
    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowRecordNotExistsForCreateRepresentation()
            throws MCSException {

        String cloudId = "noSuchRecord";
        String representationName = "schema_000001";
        String providerId = "Provider001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.createRepresentation(cloudId, representationName, providerId);
    }

    @Betamax(tape = "records_shouldCreateNewSchemaWhenNotExists")
    @Test
    public void shouldCreateNewSchemaWhenNotExists()
            throws MCSException {

        String cloudId = "BXTG2477LVX";
        String representationName = "newSchema";
        String providerId = "Provider001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        //ensure representation name does not exist
        Boolean noRepresentationName = false;
        try {
            instance.getRepresentations(cloudId, representationName);
        } catch (RepresentationNotExistsException ex) {
            noRepresentationName = true;
        }
        assertTrue(noRepresentationName);

        URI uri = instance.createRepresentation(cloudId, representationName, providerId);
        TestUtils.assertCorrectlyCreatedRepresentation(instance, uri, providerId, cloudId, representationName);
    }

    @Betamax(tape = "records_shouldThrowProviderNotExistsForCreateRepresentation")
    @Test(expected = ProviderNotExistsException.class)
    public void shouldThrowProviderNotExistsForCreateRepresentation()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        String representationName = "schema_000001";
        String providerId = "noSuchProvider";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.createRepresentation(cloudId, representationName, providerId);
    }

    @Betamax(tape = "records_shouldThrowDriverExceptionForCreateRepresentation")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForCreateRepresentation()
            throws Exception {

        String cloudId = "1DZ6HTS415W";
        String representationName = "schema77";
        String providerId = "Provider001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        instance.createRepresentation(cloudId, representationName, providerId);
    }

    //deleteRepresentation(cloudId, representationName) - deleting representation name
    @Betamax(tape = "records_shouldDeleteRepresentationName")
    @Test
    public void shouldDeleteRepresentationName()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema1";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, representationName);
        //check the representation name does not exist
        //we catch this exception here and not expect in @Test,
        //because then it could also come from deleteRepresentation method call
        Boolean noRepresentationName = false;
        try {
            instance.getRepresentations(cloudId, representationName);
        } catch (RepresentationNotExistsException ex) {
            noRepresentationName = true;
        }
        assertTrue(noRepresentationName);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForDeleteRepresentationNameWhenNoRepresentationName")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForDeleteRepresentationNameWhenNoRepresentationName()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "noSuchSchema";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, representationName);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForDeleteRepresentationNameWhenNoRecord")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForDeleteRepresentationNameWhenNoRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        String representationName = "schema1";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, representationName);
    }

    @Betamax(tape = "records_shouldThrowDriverExceptionForDeleteRepresentationName")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForDeleteRepresentationName()
            throws Exception {

        String cloudId = "J93T5R6615H";
        String representationName = "schema77";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, representationName);
    }

    //getRepresentations(cloudId, representationName)
    @Betamax(tape = "records_shouldRetrieveSchemaVersions")
    @Test
    public void shouldRetrieveSchemaVersions()
            throws RepresentationNotExistsException, MCSException {
        String cloudId = "7MZWQJF8P84";
        String representationName = "schema_000001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        List<Representation> result = instance.getRepresentations(cloudId, representationName);
        assertNotNull(result);
        //in Betamax test there are 52 versions
        assertThat(result.size(), greaterThan(1));
        for (Representation representation : result) {
            assertEquals(cloudId, representation.getCloudId());
            assertEquals(representationName, representation.getRepresentationName());
        }
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForGetRepresentationNameVersionsWhenNoRepresentationName")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationNameVersionsWhenNoRepresentationName()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "noSuchSchema";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentations(cloudId, representationName);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForGetRepresentationNameVersionsWhenNoRecord")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationNameVersionsWhenNoRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        String representationName = "schema1";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentations(cloudId, representationName);
    }

    @Betamax(tape = "records_shouldThrowDriverExceptionForGetSchemaVersions")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForGetSchemaVersions()
            throws Exception {

        String cloudId = "7MZWQJF8P84";
        String representationName = "schema_000001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, representationName);
    }

    //getRepresentation(cloudId, representationName, version)
    @Betamax(tape = "records_shouldRetrieveRepresentationVersion")
    @Test
    public void shouldRetrieveRepresentationVersion()
            throws MCSException {

        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        //this is some not-persistent version
        String version = "6e2b47e0-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        Representation representation = instance.getRepresentation(cloudId, representationName, version);
        assertNotNull(representation);
        assertEquals(cloudId, representation.getCloudId());
        assertEquals(representationName, representation.getRepresentationName());
        assertEquals(version, representation.getVersion());
    }

    //@Betamax(tape = "records_shouldRetrieveLatestRepresentationVersion")
    @Ignore
    @Test
    public void shouldRetrieveLatestRepresentationVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        String version = "LATEST";
        //this is the version of latest persistent version
        String versionCode = "88edb4d0-a2ef-11e3-89f5-1c6f653f6012";
        
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        Representation representationLatest = instance.getRepresentation(cloudId, representationName, version);
        assertNotNull(representationLatest);
        assertEquals(cloudId, representationLatest.getCloudId());
        assertEquals(representationName, representationLatest.getRepresentationName());
        assertEquals(versionCode, representationLatest.getVersion());

        //check by getting lastest persistent representation with other method
        Representation representation = instance.getRepresentation(cloudId, representationName);
        //TODO JIRA ECL-160
        //assertEquals(representationLatest, representation);
    }

    @Betamax(tape = "records_shouldTreatLatestPersistentVersionAsLatestCreated")
    @Test
    public void shouldTreatLatestPersistentVersionAsLatestCreated()
            throws MCSException, IOException {
        String providerId = "Provider001";
        String cloudId = "J93T5R6615H";
        String representationName = "schema33";
        String fileType = "text/plain";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        FileServiceClient fileService = new FileServiceClient(baseUrl);

        //create representation A
        URI uriA = instance.createRepresentation(cloudId, representationName, providerId);
        //create representation B
        URI uriB = instance.createRepresentation(cloudId, representationName, providerId);
        //obtain version codes
        String versionA = TestUtils.obtainRepresentationFromURI(instance, uriA).getVersion();
        String versionB = TestUtils.obtainRepresentationFromURI(instance, uriB).getVersion();
        //add files
        fileService.uploadFile(cloudId, representationName, versionA, new ByteArrayInputStream("fileA".getBytes()), fileType);
        fileService.uploadFile(cloudId, representationName, versionB, new ByteArrayInputStream("fileB".getBytes()), fileType);
        //persist representation B
        instance.persistRepresentation(cloudId, representationName, versionB);
        //persist representation A
        instance.persistRepresentation(cloudId, representationName, versionA);
        //check what was obtained
        Representation representation = instance.getRepresentation(cloudId, representationName);
        assertEquals(representation.getVersion(), versionB);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForGetRepresentationVersionWhenNoRecord")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationVersionWhenNoRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        String representationName = "schema22";
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForGetRepresentationVersionWhenNoRepresentationName")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationVersionWhenNoRepresentationName()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "noSuchSchema";
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForGetRepresentationVersionWhenNoSuchVersion")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationVersionWhenNoSuchVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        //there is no such version, but the UUID is valid
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6013";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowDriverExceptionForGetRepresentationVersionWhenInvalidVersion")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForGetRepresentationVersionWhenInvalidVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        //there is no such version and the UUID is invalid
        String version = "noSuchVersion";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentation(cloudId, representationName, version);
    }

    //for example when Cassandra is not working
    @Betamax(tape = "records_shouldThrowDriverExceptionForGetRepresentationVersion")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForGetRepresentationVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        String version = "6e2b47e0-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentation(cloudId, representationName, version);
    }

    //deleteRepresentation(cloudId, representationName, version)
    @Betamax(tape = "records_shouldDeleteRepresentationVersion")
    @Test
    public void shouldDeleteRepresentationVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        String version = "6e2b47e0-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, representationName, version);

        //try to get this version
        Boolean noVersion = false;
        try {
            instance.getRepresentation(cloudId, representationName, version);
        } catch (RepresentationNotExistsException ex) {
            noVersion = true;
        }
        assertTrue(noVersion);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForDeleteRepresentationVersionWhenNoRecord")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForDeleteRepresentationVersionWhenNoRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        String representationName = "schema22";
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForDeleteRepresentationVersionWhenNoRepresentationName")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForDeleteRepresentationVersionWhenNoRepresentationName()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "noSuchSchema";
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForDeleteRepresentationVersionWhenNoSuchVersion")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForDeleteRepresentationVersionWhenNoSuchVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        //there is no such version, but the UUID is valid
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6013";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowDriverExceptionForDeleteRepresentationVersionWhenInvalidVersion")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForDeleteRepresentationVersionWhenInvalidVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        //there is no such version and the UUID is invalid
        String version = "noSuchVersion";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, representationName, version);
    }

    //for example when Cassandra is not working
    @Betamax(tape = "records_shouldThrowDriverExceptionForDeleteRepresentationVersion")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForDeleteRepresentationVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        String version = "7b2349c0-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldNotAllowToDeletePersistenRepresentation")
    @Test(expected = CannotModifyPersistentRepresentationException.class)
    public void shouldNotAllowToDeletePersistenRepresentation()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        //this version is persistent (but not latest - not important)
        String version = "67565180-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        //check this representation is persistent
        Representation representation = instance.getRepresentation(cloudId, representationName, version);
        assertNotNull(representation);
        assertEquals(cloudId, representation.getCloudId());
        assertEquals(representationName, representation.getRepresentationName());
        assertEquals(version, representation.getVersion());
        assertTrue(representation.isPersistent());

        //try to delete
        instance.deleteRepresentation(cloudId, representationName, version);
    }

    //copyRepresentation
    @Betamax(tape = "records_shouldCopyNonPersistentRepresentation")
    @Test
    public void shouldCopyNonPersistentRepresentation()
            throws MCSException, IOException {

        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        //not persistent representation with two files in it
        String sourceVersion = "8e93ef30-a2ef-11e3-89f5-1c6f653f6012";
        //I fix it because if I just checked that they have the same number it could be 0
        int filesSize = 2;
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        //make a copy
        URI targetURI = instance.copyRepresentation(cloudId, representationName, sourceVersion);
        assertNotNull(targetURI);

        //get copying result
        Representation targetRepresentation = TestUtils.obtainRepresentationFromURI(instance, targetURI);
        //check that is has two files in it
        assertEquals(targetRepresentation.getFiles().size(), filesSize);
        //get the source version
        Representation sourceRepresentation = instance.getRepresentation(cloudId, representationName, sourceVersion);
        //check the versions differ
        assertNotEquals(targetRepresentation.getVersion(), sourceRepresentation.getVersion());
        //check both versions are not persistent
        assertEquals(sourceRepresentation.isPersistent(), false);
        assertEquals(targetRepresentation.isPersistent(), false);
        //check that files content does not differ
        TestUtils.assertSameFiles(targetRepresentation, sourceRepresentation);

    }

    @Betamax(tape = "records_shouldCopyPersistentRepresentation")
    @Test
    public void shouldCopyPersistentRepresentation()
            throws MCSException, IOException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        //this is some persistent version with two files in it
        String sourceVersion = "88edb4d0-a2ef-11e3-89f5-1c6f653f6012";
        //I fix it because if I just checked that they have the same number it could be 0
        int filesSize = 2;
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        //make a copy
        URI targetURI = instance.copyRepresentation(cloudId, representationName, sourceVersion);

        //check URI
        assertNotNull(targetURI);
        //get copying result
        Representation targetRepresentation = TestUtils.obtainRepresentationFromURI(instance, targetURI);
        //check that is has two files in it
        assertEquals(targetRepresentation.getFiles().size(), filesSize);
        //get the source version
        Representation sourceRepresentation = instance.getRepresentation(cloudId, representationName, sourceVersion);
        //check the versions differ
        assertNotEquals(targetRepresentation.getVersion(), sourceRepresentation.getVersion());
        //check the source is persistent and target not
        assertEquals(sourceRepresentation.isPersistent(), true);
        assertEquals(targetRepresentation.isPersistent(), false);
        //check that files content does not differ
        TestUtils.assertSameFiles(targetRepresentation, sourceRepresentation);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForCopyRepresentationWhenNoRecord")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForCopyRepresentationWhenNoRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        String representationName = "schema22";
        String version = "88edb4d0-a2ef-11e3-89f5-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.copyRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForCopyRepresentationWhenNoRepresentationName")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForCopyRepresentationWhenNoRepresentationName()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "noSuchSchema";
        String version = "88edb4d0-a2ef-11e3-89f5-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.copyRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForCopyRepresentationVersionWhenNoSuchVersion")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForCopyRepresentationVersionWhenNoSuchVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        //there is no such version, but the UUID is valid
        String version = "88edb4d0-a2ef-11e3-89f5-1c6f653f6013";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.copyRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowDriverExceptionForCopyRepresentationVersionWhenInvalidVersion")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForCopyRepresentationVersionWhenInvalidVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        //there is no such version and the UUID is invalid
        String version = "noSuchVersion";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.copyRepresentation(cloudId, representationName, version);
    }

    //for example when Cassandra is not working
    @Betamax(tape = "records_shouldThrowDriverExceptionForCopyRepresentationVersion")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForCopyRepresentationVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        String version = "7b2349c0-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.copyRepresentation(cloudId, representationName, version);
    }

    //persistRepresentation
    @Betamax(tape = "records_shouldPersistAfterAddingFiles")
    @Test
    public void shouldPersistAfterAddingFiles()
            throws MCSException, IOException {
        String providerId = "Provider001";
        String cloudId = "J93T5R6615H";
        String representationName = "schema33";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        FileServiceClient fileService = new FileServiceClient(baseUrl);
        String fileContent = "The content of the file.";
        String fileType = "text/plain";

        //create representation
        URI uriCreated = instance.createRepresentation(cloudId, representationName, providerId);
        Representation coordinates = TestUtils.parseRepresentationFromUri(uriCreated);

        //add files
        InputStream data = new ByteArrayInputStream(fileContent.getBytes());
        fileService.uploadFile(cloudId, representationName, coordinates.getVersion(), data, fileType);

        //persist representation
        URI uriPersisted = instance.persistRepresentation(cloudId, representationName, coordinates.getVersion());

        assertNotNull(uriPersisted);
        Representation persistedRepresentation = TestUtils.obtainRepresentationFromURI(instance, uriPersisted);
        assertEquals(providerId, persistedRepresentation.getDataProvider());
        assertEquals(representationName, persistedRepresentation.getRepresentationName());
        assertEquals(cloudId, persistedRepresentation.getCloudId());
        assertEquals(coordinates.getVersion(), persistedRepresentation.getVersion());
        assertEquals(persistedRepresentation.isPersistent(), true);

    }

    @Betamax(tape = "records_shouldNotPersistEmptyRepresentation")
    @Test(expected = CannotPersistEmptyRepresentationException.class)
    public void shouldNotPersistEmptyRepresentation()
            throws MCSException, IOException {
        String providerId = "Provider001";
        String cloudId = "J93T5R6615H";
        String representationName = "schema33";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        //create new representation version
        URI uri = instance.createRepresentation(cloudId, representationName, providerId);
        //obtain the version
        String version = TestUtils.parseRepresentationFromUri(uri).getVersion();
        //try to persist
        instance.persistRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldNotPersistRepresentationAgain")
    @Test(expected = CannotModifyPersistentRepresentationException.class)
    public void shouldNotPersistRepresentationAgain()
            throws MCSException, IOException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema33";
        //this version is alredy presistent
        String version = "559bc4a0-a380-11e3-857e-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        //ensure this version is persistent
        Representation representation = instance.getRepresentation(cloudId, representationName, version);
        assertEquals(representation.isPersistent(), true);

        //try to persist
        instance.persistRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsExceptionForPersistRepresentationWhenNoRecord")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsExceptionForPersistRepresentationWhenNoRecord()
            throws MCSException, IOException {
        String cloudId = "noSuchRecord";
        String representationName = "schema33";
        String version = "fece3cb0-a5fb-11e3-b4a7-50e549e85271";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.persistRepresentation(cloudId, representationName, version);
    }

    @Betamax(
            tape = "records_shouldThrowRepresentationNotExistsExceptionForPersistRepresentationWhenNoRepresentationName")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsExceptionForPersistRepresentationWhenNoRepresentationName()
            throws MCSException, IOException {
        String cloudId = "J93T5R6615H";
        String representationName = "noSuchSchema";
        String version = "fece3cb0-a5fb-11e3-b4a7-50e549e85271";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.persistRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsExceptionForPersistRepresentationWhenNoSuchVersion")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsExceptionForPersistRepresentationWhenNoSuchVersion()
            throws MCSException, IOException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema33";
        String version = "fece3cb0-a5fb-11e3-b4a7-50e549e85204";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
       
        instance.persistRepresentation(cloudId, representationName, version);
    }
    
    @Betamax(tape = "records_shouldThrowDriverExceptionForPersistRepresentationVersionWhenInvalidVersion")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForPersistRepresentationVersionWhenInvalidVersion()
            throws MCSException, IOException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema33";
        String version = "noSuchVersion";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
       
        instance.persistRepresentation(cloudId, representationName, version);
    }
    

    //for example when Cassandra is not working
    @Betamax(tape = "records_shouldThrowDriverExceptionForPersistRepresentation")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForPersistRepresentation()
            throws MCSException, IOException {
        String cloudId = "7MZWQJF8P84";
        String representationName = "schema_000001";
        String version = "fece3cb0-a5fb-11e3-b4a7-50e549e85271";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.persistRepresentation(cloudId, representationName, version);
    }


}
