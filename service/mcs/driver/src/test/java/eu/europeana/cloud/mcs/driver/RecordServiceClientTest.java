package eu.europeana.cloud.mcs.driver;

import co.freeside.betamax.Betamax;
import co.freeside.betamax.Recorder;
import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.*;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.number.OrderingComparisons.greaterThan;
import static org.junit.Assert.*;

public class RecordServiceClientTest {

    @Rule
    public Recorder recorder = new Recorder();

    // TODO clean
    // this is only needed for recording tests
    private static final String BASE_URL_ISTI = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/";
    private static final String BASE_URL_LOCALHOST = "http://localhost:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT";

    private final String baseUrl = BASE_URL_ISTI;

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
    private static final String NON_EXISTING_REPRESENTATION_NAME_2 = "NON_EXISTING_REPRESENTATION_NAME_2_12";

    private static final String username = "Cristiano";
    private static final String password = "Ronaldo";

    // getRecord
    @Betamax(tape = "records_shouldRetrieveRecord")
    @Test
    public void shouldRetrieveRecord() throws MCSException {

        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        Record record = instance.getRecord(CLOUD_ID);
        assertNotNull(record);
        assertEquals(CLOUD_ID, record.getCloudId());
    }

    @Betamax(tape = "records_shouldThrowRecordNotExistsForGetRecord")
    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowRecordNotExistsForGetRecord() throws MCSException {
        String cloudId = "noSuchRecord";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.getRecord(cloudId);
    }

    // deleteRecord
    // this test could be better with Betamax 2.0 ability to hold state (not yet
    // in main maven repository)
    // @Betamax(tape = "records_shouldDeleteRecord")
    // RECORDS CANNOT BE DELETED as of v2, so this test is disabled
    @Ignore
    @Test
    public void shouldDeleteRecord() throws MCSException {

        String cloudId = "231PJ0QGW6N";
        String representationName = "schema77";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        // delete record
        instance.deleteRecord(cloudId);

        // check that there are not representations for this record
        // we only check one representationName, because there is no method to
        // just get all representations
        List<Representation> representations = instance.getRepresentations(
                cloudId, representationName);
        assertEquals(representations.size(), 0);

    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForDeleteRecordWhenNoRepresentations")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForDeleteRecordWhenNoRepresentations()
            throws MCSException {

        String cloudId = "25DG622J4VM";
        String representationName = "schema1";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        // check that there are not representations for this record
        // we only check one representationName, because there is no method to
        // just get all representations
        List<Representation> representations = instance.getRepresentations(
                cloudId, representationName);
        assertEquals(representations.size(), 0);

        // delete record
        instance.deleteRecord(cloudId);
    }

    @Betamax(tape = "records_shouldThrowRecordNotExistsForDeleteRecord")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRecordNotExistsForDeleteRecord() throws MCSException {

        String cloudId = "noSuchRecord";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.deleteRecord(cloudId);
    }


    // getRepresentations(cloudId)
    @Betamax(tape = "records_shouldRetrieveRepresentations")
    @Test
    public void shouldRetrieveRepresentations() throws MCSException {

        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        List<Representation> representationList = instance
                .getRepresentations(CLOUD_ID);
        assertNotNull(representationList);
        // in this scenario we have 3 persistent representations, 2 in one
        // representation name and 1 in another, thus we want to get 2
        assertEquals(representationList.size(), 2);
        for (Representation representation : representationList) {
            assertEquals(CLOUD_ID, representation.getCloudId());
            assertTrue(representation.isPersistent());
        }
    }

    @Betamax(tape = "records_shouldThrowRecordNotExistsForGetRepresentations")
    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowRecordNotExistsForGetRepresentations()
            throws MCSException {
        String cloudId = "noSuchRecord";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.getRepresentations(cloudId);
    }


    // getRepresentations(cloudId, representationName)
    @Betamax(tape = "records_shouldRetrieveLastPersistentRepresentationForRepresentationName")
    @Test
    public void shouldRetrieveLastPersistentRepresentationForRepresentationName()
            throws MCSException {
        // String cloudId = "J93T5R6615H";
        // String representationName = "schema1";
        // //the last persisent representation
        // String version = "acf7a040-9587-11e3-8f2f-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);
        Representation representation = instance.getRepresentation(CLOUD_ID,
                REPRESENTATION_NAME);

        assertNotNull(representation);
        // assertEquals(cloudId, representation.getCloudId());
        // assertEquals(representationName,
        // representation.getRepresentationName());
        // assertEquals(version, representation.getVersion());
        assertTrue(representation.isPersistent());
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForGetRepresentationForRepresentationNameWhenNoRepresentationName")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationForRepresentationNameWhenNoRepresentationName()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        // no representation for this representation name
        String representationName = "noSuchSchema";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.getRepresentation(cloudId, representationName);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForGetRepresentationForRepresentationNameWhenNoPersistent")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationForRepresentationNameWhenNoPersistent()
            throws MCSException {
        String cloudId = "GWV0RHNSSGJ";
        // there are representations for this representation name, but none is
        // persistent
        String representationName = "schema1";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.getRepresentation(cloudId, representationName);
    }

    @Betamax(tape = "records_shouldThrowRecordNotExistsForCreateRepresentation")
    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowRecordNotExistsForCreateRepresentation()
            throws MCSException {

        String cloudId = "noSuchRecord";
        String representationName = "schema_000001";
        String providerId = "Provider001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.createRepresentation(cloudId, representationName, providerId);
    }

    @Betamax(tape = "records_shouldCreateNewSchemaWhenNotExists")
    @Test
    public void shouldCreateNewSchemaWhenNotExists() throws MCSException {

        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        // ensure representation name does not exist
        Boolean noRepresentationName = false;
        try {
            instance.getRepresentations(CLOUD_ID,
                    NON_EXISTING_REPRESENTATION_NAME);
        } catch (RepresentationNotExistsException ex) {
            noRepresentationName = true;
        }
        assertTrue(noRepresentationName);

        URI uri = instance.createRepresentation(CLOUD_ID,
                NON_EXISTING_REPRESENTATION_NAME, PROVIDER_ID);
        TestUtils.assertCorrectlyCreatedRepresentation(instance, uri,
                PROVIDER_ID, CLOUD_ID, NON_EXISTING_REPRESENTATION_NAME);
    }

    @Betamax(tape = "records_shouldThrowProviderNotExistsForCreateRepresentation")
    @Test(expected = ProviderNotExistsException.class)
    public void shouldThrowProviderNotExistsForCreateRepresentation()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        String representationName = "schema_000001";
        String providerId = "noSuchProvider";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.createRepresentation(cloudId, representationName, providerId);
    }


    // deleteRepresentation(cloudId, representationName) - deleting
    // representation name
    // @Betamax(tape = "records_shouldDeleteRepresentationName")
    // @Test
    public void shouldDeleteRepresentationName() throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema1";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.deleteRepresentation(cloudId, representationName);
        // check the representation name does not exist
        // we catch this exception here and not expect in @Test,
        // because then it could also come from deleteRepresentation method call
        Boolean noRepresentationName = false;
        try {
            instance.getRepresentations(cloudId, representationName);
        } catch (RepresentationNotExistsException ex) {
            noRepresentationName = true;
        }
        assertTrue(noRepresentationName);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForDeleteRepresentationNameWhenNoRepresentationName")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRepresentationNotExistsForDeleteRepresentationNameWhenNoRepresentationName()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "noSuchSchema";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.deleteRepresentation(cloudId, representationName);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForDeleteRepresentationNameWhenNoRecord")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRepresentationNotExistsForDeleteRepresentationNameWhenNoRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        String representationName = "schema1";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.deleteRepresentation(cloudId, representationName);
    }


    // getRepresentations(cloudId, representationName)
    @Betamax(tape = "records_shouldRetrieveSchemaVersions")
    @Test
    public void shouldRetrieveSchemaVersions()
            throws RepresentationNotExistsException, MCSException {

        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        List<Representation> result = instance.getRepresentations(CLOUD_ID,
                REPRESENTATION_NAME);
        assertNotNull(result);
        // in Betamax test there are more than 1 versions
        assertThat(result.size(), greaterThan(1));
        for (Representation representation : result) {
            assertEquals(CLOUD_ID, representation.getCloudId());
            assertEquals(REPRESENTATION_NAME,
                    representation.getRepresentationName());
        }
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForGetRepresentationNameVersionsWhenNoRepresentationName")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationNameVersionsWhenNoRepresentationName()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "noSuchSchema";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.getRepresentations(cloudId, representationName);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForGetRepresentationNameVersionsWhenNoRecord")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationNameVersionsWhenNoRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        String representationName = "schema1";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.getRepresentations(cloudId, representationName);
    }


    // getRepresentation(cloudId, representationName, version)
    @Betamax(tape = "records_shouldRetrieveRepresentationVersion")
    @Test
    public void shouldRetrieveRepresentationVersion() throws MCSException {

        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        Representation representation = instance.getRepresentation(CLOUD_ID,
                REPRESENTATION_NAME, VERSION);
        assertNotNull(representation);
        assertEquals(CLOUD_ID, representation.getCloudId());
        assertEquals(REPRESENTATION_NAME,
                representation.getRepresentationName());
        assertEquals(VERSION, representation.getVersion());
    }

    // @Betamax(tape = "records_shouldRetrieveLatestRepresentationVersion")
    @Ignore
    @Test
    public void shouldRetrieveLatestRepresentationVersion() throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        String version = "LATEST";
        // this is the version of latest persistent version
        String versionCode = "88edb4d0-a2ef-11e3-89f5-1c6f653f6012";

        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        Representation representationLatest = instance.getRepresentation(
                cloudId, representationName, version);
        assertNotNull(representationLatest);
        assertEquals(cloudId, representationLatest.getCloudId());
        assertEquals(representationName,
                representationLatest.getRepresentationName());
        assertEquals(versionCode, representationLatest.getVersion());

        // check by getting latest persistent representation with other method
        Representation representation = instance.getRepresentation(cloudId,
                representationName);
        // TODO JIRA ECL-160
        // assertEquals(representationLatest, representation);
    }

    @Betamax(tape = "records_shouldTreatLatestPersistentVersionAsLatestCreated")
    @Test
    public void shouldTreatLatestPersistentVersionAsLatestCreated()
            throws MCSException, IOException {

        String fileType = "text/plain";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);
        FileServiceClient fileService = new FileServiceClient(baseUrl,
                username, password);

        // create representation A
        URI uriA = instance.createRepresentation(CLOUD_ID, REPRESENTATION_NAME,
                PROVIDER_ID);
        // create representation B
        URI uriB = instance.createRepresentation(CLOUD_ID, REPRESENTATION_NAME,
                PROVIDER_ID);
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

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForGetRepresentationVersionWhenNoRecord")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationVersionWhenNoRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        String representationName = "schema22";
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.getRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForGetRepresentationVersionWhenNoRepresentationName")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationVersionWhenNoRepresentationName()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "noSuchSchema";
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.getRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForGetRepresentationVersionWhenNoSuchVersion")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationVersionWhenNoSuchVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        // there is no such version, but the UUID is valid
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6013";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.getRepresentation(cloudId, representationName, version);
    }


    // deleteRepresentation(cloudId, representationName, version)
    @Betamax(tape = "records_shouldDeleteRepresentationVersion")
    @Test
    public void shouldDeleteRepresentationVersion() throws MCSException {
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        URI newReprURI = instance.createRepresentation(CLOUD_ID,
                REPRESENTATION_NAME, PROVIDER_ID);
        Representation repr = TestUtils.parseRepresentationFromUri(newReprURI);

        instance.deleteRepresentation(CLOUD_ID, REPRESENTATION_NAME,
                repr.getVersion());

        // try to get this version
        Boolean noVersion = false;
        try {
            instance.getRepresentation(CLOUD_ID, REPRESENTATION_NAME,
                    repr.getVersion());
        } catch (RepresentationNotExistsException ex) {
            noVersion = true;
        }
        assertTrue(noVersion);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForDeleteRepresentationVersionWhenNoRecord")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRepresentationNotExistsForDeleteRepresentationVersionWhenNoRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        String representationName = "schema22";
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.deleteRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForDeleteRepresentationVersionWhenNoRepresentationName")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRepresentationNotExistsForDeleteRepresentationVersionWhenNoRepresentationName()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "noSuchSchema";
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.deleteRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForDeleteRepresentationVersionWhenNoSuchVersion")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRepresentationNotExistsForDeleteRepresentationVersionWhenNoSuchVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        // there is no such version, but the UUID is valid
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6013";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.deleteRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForDeleteRepresentationVersionWhenInvalidVersion")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForDeleteRepresentationVersionWhenInvalidVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        // there is no such version and the UUID is invalid
        String version = "noSuchVersion";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.deleteRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldNotAllowToDeletePersistenRepresentation")
    @Test(expected = CannotModifyPersistentRepresentationException.class)
    public void shouldNotAllowToDeletePersistenRepresentation()
            throws MCSException {

        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

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
        instance.deleteRepresentation(CLOUD_ID, REPRESENTATION_NAME,
                persistedRepr.getVersion());
    }

    // copyRepresentation
    @Betamax(tape = "records_shouldCopyNonPersistentRepresentation")
    @Test
    public void shouldCopyNonPersistentRepresentation() throws MCSException,
            IOException {

        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        // make a non persistent version
        URI sourceReprURI = instance.copyRepresentation(CLOUD_ID,
                REPRESENTATION_NAME, VERSION);
        assertNotNull(sourceReprURI);
        Representation sourceRepr = TestUtils.obtainRepresentationFromURI(
                instance, sourceReprURI);
        // make sure is not persistent
        assertTrue(!sourceRepr.isPersistent());

        int currentFileSize = sourceRepr.getFiles().size();
        assertTrue(currentFileSize > 0);

        // make a copy of the Non persistent version
        URI targetURI = instance.copyRepresentation(CLOUD_ID,
                REPRESENTATION_NAME, sourceRepr.getVersion());

        // get copying result
        Representation targetRepresentation = TestUtils
                .obtainRepresentationFromURI(instance, targetURI);

        // check that is has two files in it
        assertEquals(targetRepresentation.getFiles().size(), currentFileSize);
        // get the source version
        Representation sourceRepresentation = instance.getRepresentation(
                CLOUD_ID, REPRESENTATION_NAME, sourceRepr.getVersion());
        // check the versions differ
        assertNotEquals(targetRepresentation.getVersion(),
                sourceRepresentation.getVersion());
        // check both versions are not persistent
        assertEquals(sourceRepresentation.isPersistent(), false);
        assertEquals(targetRepresentation.isPersistent(), false);
        // check that files content does not differ
        TestUtils.assertSameFiles(targetRepresentation, sourceRepresentation);

    }

    @Betamax(tape = "records_shouldCopyPersistentRepresentation")
    @Test
    public void shouldCopyPersistentRepresentation() throws MCSException,
            IOException {

        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        Representation currentRepresentation = instance.getRepresentation(
                CLOUD_ID, REPRESENTATION_NAME, VERSION);
        assertEquals(currentRepresentation.isPersistent(), true);
        int currentFileSize = currentRepresentation.getFiles().size();
        assertTrue(currentFileSize > 0);

        // now copy the persistent repr
        URI copiedRerpURI = instance.copyRepresentation(CLOUD_ID,
                REPRESENTATION_NAME, currentRepresentation.getVersion());
        Representation copiedRerp = TestUtils.obtainRepresentationFromURI(
                instance, copiedRerpURI);

        // check the copy is not perst
        assertTrue(!copiedRerp.isPersistent());

        // check that is has the same files in it
        assertEquals(copiedRerp.getFiles().size(), currentFileSize);
        // get the source version
        Representation sourceRepresentation = currentRepresentation;
        // check the versions differ
        assertNotEquals(copiedRerp.getVersion(),
                sourceRepresentation.getVersion());
        // check the source is persistent and target not
        assertEquals(sourceRepresentation.isPersistent(), true);
        assertEquals(copiedRerp.isPersistent(), false);
        // check that files content does not differ
        TestUtils.assertSameFiles(copiedRerp, sourceRepresentation);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForCopyRepresentationWhenNoRecord")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRepresentationNotExistsForCopyRepresentationWhenNoRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        String representationName = "schema22";
        String version = "88edb4d0-a2ef-11e3-89f5-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.copyRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForCopyRepresentationWhenNoRepresentationName")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRepresentationNotExistsForCopyRepresentationWhenNoRepresentationName()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "noSuchSchema";
        String version = "88edb4d0-a2ef-11e3-89f5-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.copyRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsForCopyRepresentationVersionWhenNoSuchVersion")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRepresentationNotExistsForCopyRepresentationVersionWhenNoSuchVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        // there is no such version, but the UUID is valid
        String version = "88edb4d0-a2ef-11e3-89f5-1c6f653f6013";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.copyRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowAccessDeniedForCopyRepresentationVersionWhenInvalidVersion")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowAccessDeniedForCopyRepresentationVersionWhenInvalidVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema22";
        // there is no such version and the UUID is invalid
        String version = "noSuchVersion";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.copyRepresentation(cloudId, representationName, version);
    }


    // persistRepresentation
    @Betamax(tape = "records_shouldPersistAfterAddingFiles")
    @Test
    public void shouldPersistAfterAddingFiles() throws MCSException,
            IOException {

        String representationName = "schema33";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);
        FileServiceClient fileService = new FileServiceClient(baseUrl,
                username, password);
        String fileContent = "The content of the file.";
        String fileType = "text/plain";

        // create representation
        URI uriCreated = instance.createRepresentation(CLOUD_ID,
                representationName, PROVIDER_ID);
        Representation coordinates = TestUtils
                .parseRepresentationFromUri(uriCreated);

        // add files
        InputStream data = new ByteArrayInputStream(fileContent.getBytes());
        URI fileURI = fileService.uploadFile(CLOUD_ID, representationName,
                coordinates.getVersion(), data, fileType);

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
        assertEquals(persistedRepresentation.isPersistent(), true);
    }

    @Betamax(tape = "records_shouldNotPersistEmptyRepresentation")
    @Test(expected = CannotPersistEmptyRepresentationException.class)
    public void shouldNotPersistEmptyRepresentation() throws MCSException,
            IOException {

        String representationName = "schema33";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        // create new representation version
        URI uri = instance.createRepresentation(CLOUD_ID, representationName,
                PROVIDER_ID);
        // obtain the version
        String version = TestUtils.parseRepresentationFromUri(uri).getVersion();
        // try to persist
        instance.persistRepresentation(CLOUD_ID, representationName, version);
    }

    @Betamax(tape = "records_shouldNotPersistRepresentationAgain")
    @Test(expected = CannotModifyPersistentRepresentationException.class)
    public void shouldNotPersistRepresentationAgain() throws MCSException,
            IOException {

        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        // ensure this version is persistent
        Representation representation = instance.getRepresentation(CLOUD_ID, REPRESENTATION_NAME);
        assertEquals(representation.isPersistent(), true);

        // try to persist
        instance.persistRepresentation(CLOUD_ID, REPRESENTATION_NAME, representation.getVersion());
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsExceptionForPersistRepresentationWhenNoRecord")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRepresentationNotExistsExceptionForPersistRepresentationWhenNoRecord()
            throws MCSException, IOException {
        String cloudId = "noSuchRecord";
        String representationName = "schema33";
        String version = "fece3cb0-a5fb-11e3-b4a7-50e549e85271";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.persistRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsExceptionForPersistRepresentationWhenNoRepresentationName")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRepresentationNotExistsExceptionForPersistRepresentationWhenNoRepresentationName()
            throws MCSException, IOException {
        String cloudId = "J93T5R6615H";
        String representationName = "noSuchSchema";
        String version = "fece3cb0-a5fb-11e3-b4a7-50e549e85271";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.persistRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExistsExceptionForPersistRepresentationWhenNoSuchVersion")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRepresentationNotExistsExceptionForPersistRepresentationWhenNoSuchVersion()
            throws MCSException, IOException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema33";
        String version = "fece3cb0-a5fb-11e3-b4a7-50e549e85204";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.persistRepresentation(cloudId, representationName, version);
    }

    @Betamax(tape = "records_shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForPersistRepresentationVersionWhenInvalidVersion")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForPersistRepresentationVersionWhenInvalidVersion()
            throws MCSException, IOException {
        String cloudId = "J93T5R6615H";
        String representationName = "schema33";
        String version = "noSuchVersion";
        RecordServiceClient instance = new RecordServiceClient(baseUrl,
                username, password);

        instance.persistRepresentation(cloudId, representationName, version);
    }

    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    @Betamax(tape = "records_shouldThrowAccessDeniedOrObjectDoesNotExistExceptionWhileTryingToUpdatePermissions")
    public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionWhileTryingToUpdatePermissions()
            throws MCSException, IOException {
        RecordServiceClient client = new RecordServiceClient("http://localhost:8080/mcs");
        client.grantPermissionsToVersion(CLOUD_ID, REPRESENTATION_NAME, VERSION, "user", Permission.READ);
    }

    @Test
    @Betamax(tape = "records_shouldUpdatePermissionsWhenAuthorizationHeaderIsCorrect")
    public void shouldUpdatePermissionsWhenAuthorizationHeaderIsCorrect()
            throws MCSException, IOException {
        String correctHeaderValue = "Basic YWRtaW46YWRtaW4=";
        RecordServiceClient client = new RecordServiceClient("http://localhost:8080/mcs");
        client.useAuthorizationHeader(correctHeaderValue);
        client.grantPermissionsToVersion("FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ", "TIFF", "86318b00-6377-11e5-a1c6-90e6ba2d09ef", "user", Permission.READ);
    }

    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    @Betamax(tape = "records_accessDeniedRequest")
    public void shouldThrowAccessDeniedExceptionWhenAuthorizationHeaderIsNotCorrect()
            throws MCSException, IOException {
        String headerValue = "Basic wrongHeaderValue";
        RecordServiceClient client = new RecordServiceClient("http://localhost:8080/mcs");
        client.useAuthorizationHeader(headerValue);
        client.grantPermissionsToVersion("FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ", "TIFF", "86318b00-6377-11e5-a1c6-90e6ba2d09ef", "user", Permission.READ);
    }

    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    @Betamax(tape = "records_shouldThrowAccessDeniedOrObjectDoesNotExistExceptionWhileTryingToRevokePermissions")
    public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionWhileTryingToRevokePermissions()
            throws MCSException, IOException {
        RecordServiceClient client = new RecordServiceClient("http://localhost:8080/mcs");
        client.revokePermissionsToVersion(CLOUD_ID, REPRESENTATION_NAME, VERSION, "user", Permission.READ);
    }

    @Test
    @Betamax(tape = "records_shouldRevokePermissionsWhenAuthorizationHeaderIsCorrect")
    public void shouldRevokePermissionsWhenAuthorizationHeaderIsCorrect()
            throws MCSException, IOException {
        String correctHeaderValue = "Basic YWRtaW46YWRtaW4=";
        RecordServiceClient client = new RecordServiceClient("http://localhost:8080/mcs");
        client.useAuthorizationHeader(correctHeaderValue);
        client.revokePermissionsToVersion("FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ", "TIFF", "86318b00-6377-11e5-a1c6-90e6ba2d09ef", "user", Permission.READ);
    }

    @Test(expected = DriverException.class)
    @Betamax(tape = "records_shouldThrowDriverExceptionWhileMcsIsNotAvailable")
    public void shouldThrowMcsExceptionWhileMcsIsNotAvailable() throws MCSException {
        RecordServiceClient client = new RecordServiceClient("http://localhost:8080/mcs");
        client.grantPermissionsToVersion(CLOUD_ID, REPRESENTATION_NAME, VERSION, "user", Permission.READ);
    }

    @Test
    @Betamax(tape = "records_shouldCreateNewRepresentationAndUploadFile")
    public void shouldCreateNewRepresentationAndUploadAFile() throws IOException, FileNotFoundException, MCSException {
        RecordServiceClient client = new RecordServiceClient("http://localhost:8080/mcs", "admin", "admin");
        InputStream stream = new ByteArrayInputStream("example File Content".getBytes(StandardCharsets.UTF_8));
        client.createRepresentation("FGDNTHPJQAUTEIGAHOALM2PMFSDRD726U5LNGMPYZZ34ZNVT5YGA", "sampleRepresentationName9", "sampleProvider", stream, "fileName", "mediaType");
    }

    ;

    @Test
    @Betamax(tape = "records_shouldCreateNewRepresentationAndUploadFile")
    public void shouldCreateNewRepresentationAndUploadAFile_1() throws IOException, FileNotFoundException, MCSException {
        RecordServiceClient client = new RecordServiceClient("http://localhost:8080/mcs", "admin", "admin");
        InputStream stream = new ByteArrayInputStream("example File Content".getBytes(StandardCharsets.UTF_8));
        client.createRepresentation("FGDNTHPJQAUTEIGAHOALM2PMFSDRD726U5LNGMPYZZ34ZNVT5YGA", "sampleRepresentationName9", "sampleProvider", stream, "mediaType");
    }

    @Betamax(tape = "records_shouldRetrieveRepresentationByRevision")
    @Test
    public void shouldRetrieveRepresentationRevision() throws MCSException {

        RecordServiceClient instance = new RecordServiceClient("http://localhost:8080/mcs", "admin", "admin");
        // retrieve representation by revision
        List<Representation> representations = instance.getRepresentationsByRevision("Z6DX3RWCEFUUSGRUWP6QZWRIZKY7HI5Y7H4UD3OQVB3SRPAUVZHA", "REPRESENTATION1", "Revision_2", "Revision_Provider", "2018-08-28T07:13:34.658");
        assertNotNull(representations);
        assertTrue(representations.size() == 1);
        assertEquals("REPRESENTATION1",
                representations.get(0).getRepresentationName());
        assertEquals("68b4cc30-aa8d-11e8-8289-1c6f653f9042", representations.get(0).getVersion());
    }

    @Betamax(tape = "records_shouldThrowRepresentationNotExist")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExists() throws MCSException {

        RecordServiceClient instance = new RecordServiceClient("http://localhost:8080/mcs", "admin", "admin");
        instance.getRepresentationsByRevision("Z6DX3RWCEFUUSGRUWP6QZWRIZKY7HI5Y7H4UD3OQVB3SRPAUVZHA", "REPRESENTATION2", "Revision_2", "Revision_Provider", "2018-08-28T07:13:34.658");

    }
}
