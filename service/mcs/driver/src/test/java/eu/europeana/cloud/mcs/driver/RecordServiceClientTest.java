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
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.List;
import static org.hamcrest.CoreMatchers.is;
import org.junit.Rule;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.hamcrest.number.OrderingComparisons.greaterThan;
import static org.junit.Assert.assertThat;
import org.junit.Ignore;
import org.junit.Test;
import eu.europeana.cloud.mcs.driver.TestUtils;
import static org.junit.Assert.assertNotEquals;

public class RecordServiceClientTest {

    @Rule
    public Recorder recorder = new Recorder();

    //TODO clean
    //this is only needed for recording tests
    private final String baseUrl = "http://localhost:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/";


    //getRecord
    @Betamax(tape = "records/getRecordSuccess")
    @Test
    public void shouldRetrieveRecord()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        Record record = instance.getRecord(cloudId);
        assertNotNull(record);
        assertEquals(cloudId, record.getCloudId());
    }


    @Betamax(tape = "records/getRecordNoRecord")
    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowRecordNotExistsForGetRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRecord(cloudId);
    }


    @Betamax(tape = "records/getRecordInternalServerError")
    @Test(expected = DriverException.class)
    public void shouldThrowInternalServerErrorForGetRecord()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRecord(cloudId);
    }


    //deleteRecord
    //this test could be better with Betamax 2.0 ability to hold state (not yet in main maven repository)
    @Betamax(tape = "records/deleteRecordSuccess")
    @Test
    public void shouldDeleteRecord()
            throws MCSException {

        String cloudId = "1DZ6HTS415W";
        String schema = "schema77";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        //delete record
        instance.deleteRecord(cloudId);

        //check that there are not representations for this record
        //we only check one schema, because there is no method to just get all representations
        List<Representation> representations = instance.getRepresentations(cloudId, schema);
        assertEquals(representations.size(), 0);

    }


    @Betamax(tape = "records/deleteRecordNoRepresentations")
    @Test()
    public void shouldNotComplainAboutDeletingRecordWithNoRepresentations()
            throws MCSException {

        String cloudId = "1DZ6HTS415W";
        String schema = "schema77";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        //check that there are not representations for this record
        //we only check one schema, because there is no method to just get all representations
        List<Representation> representations = instance.getRepresentations(cloudId, schema);
        assertEquals(representations.size(), 0);

        //delete record
        instance.deleteRecord(cloudId);
    }


    @Betamax(tape = "records/deleteRecordNoRecord")
    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowRecordNotExistsForDeleteRecord()
            throws MCSException {

        String cloudId = "noSuchRecord";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRecord(cloudId);
    }


    @Betamax(tape = "records/deleteRecordInternalServerError")
    @Test(expected = DriverException.class)
    public void shouldThrowInternalServerErrorForDeleteRecord()
            throws MCSException {
        String cloudId = "1DZ6HTS415W";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRecord(cloudId);
    }


    //getRepresentations(cloudId)
    @Betamax(tape = "records/getRepresentationsSuccess")
    @Test
    public void shouldRetrieveRepresentations()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        List<Representation> representationList = instance.getRepresentations(cloudId);
        assertNotNull(representationList);
        //in this scenario we have 3 persistent representations, 2 in one schema and 1 in another, thus we want to get 2
        assertEquals(representationList.size(), 2);
        for (Representation representation : representationList) {
            assertEquals(cloudId, representation.getCloudId());
            assertTrue(representation.isPersistent());
        }
    }


    @Betamax(tape = "records/getRepresentationsNoRecord")
    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowRecordNotExistsForGetRepresentations()
            throws MCSException {
        String cloudId = "noSuchRecord";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentations(cloudId);
    }


    @Betamax(tape = "records/getRepresentationsInternalServerError")
    @Test(expected = DriverException.class)
    public void shouldThrowInternalServerErrorForGetRepresentations()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentations(cloudId);
    }


    //getRepresentations(cloudId, schema)
    @Betamax(tape = "records/getRepresentationForSchemaSuccess")
    @Test
    public void shouldRetrieveLastPersistentRepresentationForSchema()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String schema = "schema1";
        //the last persisent representation
        String version = "acf7a040-9587-11e3-8f2f-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        Representation representation = instance.getRepresentation(cloudId, schema);

        assertNotNull(representation);
        assertEquals(cloudId, representation.getCloudId());
        assertEquals(schema, representation.getRepresentationName());
        assertEquals(version, representation.getVersion());
        assertTrue(representation.isPersistent());
    }


    @Betamax(tape = "records/getRepresentationForSchemaNoSchema")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationForSchemaWhenNoSchema()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        //no representation for this schema
        String schema = "noSuchSchema";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentation(cloudId, schema);
    }


    @Betamax(tape = "records/getRepresentationForSchemaNoPersistent")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationForSchemaWhenNoPersistent()
            throws MCSException {
        String cloudId = "GWV0RHNSSGJ";
        //there are representations for this schema, but none is persistent
        String schema = "schema1";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentation(cloudId, schema);
    }


    @Betamax(tape = "records/getRepresentationForSchemaInternalServerError")
    @Test(expected = DriverException.class)
    public void shouldThrowInternalServerErrorForGetRepresentationForSchema()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String schema = "schema1";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentation(cloudId, schema);
    }


    //create representation
    @Betamax(tape = "records/createRepresentationSuccess")
    @Test
    public void shouldCreateRepresentation()
            throws MCSException {

        String cloudId = "1DZ6HTS415W";
        String schema = "schema77";
        String providerId = "Provider001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        URI uri = instance.createRepresentation(cloudId, schema, providerId);
        TestUtils.assertCorrectlyCreatedRepresentation(instance, uri, providerId, cloudId, schema);

    }


    @Betamax(tape = "records/createRepresentationRecordNotExists")
    @Test(expected = RecordNotExistsException.class)
    public void shouldThrowRecordNotExistsForCreateRepresentation()
            throws MCSException {

        String cloudId = "noSuchRecord";
        String schema = "schema_000001";
        String providerId = "Provider001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.createRepresentation(cloudId, schema, providerId);
    }


    @Betamax(tape = "records/createRepresentationSchemaNotExists")
    @Test
    public void shouldCreateNewSchemaWhenNotExists()
            throws MCSException {

        String cloudId = "BXTG2477LVX";
        String schema = "newSchema";
        String providerId = "Provider001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        //ensure schema not exists
        Boolean schemaNotExists = false;
        try {
            instance.getRepresentations(cloudId, schema);
        } catch (RepresentationNotExistsException ex) {
            schemaNotExists = true;
        }
        assertTrue(schemaNotExists);

        URI uri = instance.createRepresentation(cloudId, schema, providerId);
        TestUtils.assertCorrectlyCreatedRepresentation(instance, uri, providerId, cloudId, schema);
    }


    @Betamax(tape = "records/createRepresentationNoProvider")
    @Test(expected = ProviderNotExistsException.class)
    public void shouldThrowProviderNotExistsForCreateRepresentation()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        String schema = "schema_000001";
        String providerId = "noSuchProvider";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.createRepresentation(cloudId, schema, providerId);
    }


    @Betamax(tape = "records/createRepresentationInternalServerError")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForCreateRepresentation()
            throws Exception {

        String cloudId = "1DZ6HTS415W";
        String schema = "schema77";
        String providerId = "Provider001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        instance.createRepresentation(cloudId, schema, providerId);
    }


    //deleteRepresentation(cloudId, schema) - deleting schema
    @Betamax(tape = "records/deleteSchemaSuccess")
    @Test
    public void shouldDeleteSchema()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String schema = "schema1";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, schema);
        //check the schema does not exist
        //we catch this exception here and not expect in @Test,
        //because then it could also come from deleteRepresentation method call
        Boolean noSchema = false;
        try {
            instance.getRepresentations(cloudId, schema);
        } catch (RepresentationNotExistsException ex) {
            noSchema = true;
        }
        assertTrue(noSchema);
    }


    @Betamax(tape = "records/deleteSchemaNoSchema")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForDeleteSchemaWhenNoSchema()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String schema = "noSuchSchema";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, schema);
    }


    @Betamax(tape = "records/deleteSchemaNoRecord")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForDeleteSchemaWhenNoRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        String schema = "schema1";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, schema);
    }


    @Betamax(tape = "records/deleteSchemaInternalServerError")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForDeleteSchema()
            throws Exception {

        String cloudId = "J93T5R6615H";
        String schema = "schema77";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, schema);
    }


    //getRepresentations(cloudId, schema)
    @Betamax(tape = "records/getSchemaVersionsSuccess")
    @Test
    public void shouldRetrieveSchemaVersions()
            throws RepresentationNotExistsException, MCSException {
        String cloudId = "7MZWQJF8P84";
        String schema = "schema_000001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        List<Representation> result = instance.getRepresentations(cloudId, schema);
        assertNotNull(result);
        //in Betamax test there are 52 versions
        assertThat(result.size(), greaterThan(1));
        for (Representation representation : result) {
            assertEquals(cloudId, representation.getCloudId());
            assertEquals(schema, representation.getRepresentationName());
        }
    }


    @Betamax(tape = "records/getSchemaVersionsNoSchema")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetSchemaVersionsWhenNoSchema()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String schema = "noSuchSchema";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentations(cloudId, schema);
    }


    @Betamax(tape = "records/getSchemaVersionsNoRecord")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetSchemaVersionsWhenNoRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        String schema = "schema1";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentations(cloudId, schema);
    }


    @Betamax(tape = "records/getSchemaVersionsInternalServerError")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForGetSchemaVersions()
            throws Exception {

        String cloudId = "7MZWQJF8P84";
        String schema = "schema_000001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, schema);
    }


    //getRepresentation(cloudId, schema, version)
    @Betamax(tape = "records/getRepresentationSuccess")
    @Test
    public void shouldRetrieveRepresentationVersion()
            throws MCSException {

        String cloudId = "J93T5R6615H";
        String schema = "schema22";
        //this is some not-persistent version
        String version = "6e2b47e0-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        Representation representation = instance.getRepresentation(cloudId, schema, version);
        assertNotNull(representation);
        assertEquals(cloudId, representation.getCloudId());
        assertEquals(schema, representation.getRepresentationName());
        assertEquals(version, representation.getVersion());
    }


    //@Betamax(tape = "records/getRepresentationLatestSuccess")
    @Ignore
    @Test
    public void shouldRetrieveLatestRepresentationVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String schema = "schema22";
        String version = "LATEST";
        //this is the version of latest persistent version

        //TODO
        //String versionCode = "74cc8410-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        Representation representationLatest = instance.getRepresentation(cloudId, schema, version);
        assertNotNull(representationLatest);
        assertEquals(cloudId, representationLatest.getCloudId());
        assertEquals(schema, representationLatest.getRepresentationName());
        //assertEquals(versionCode, representationLatest.getVersion());

        //check by getting lastest persistent representation with other method
        Representation representation = instance.getRepresentation(cloudId, schema);
        //TODO WTF
        //assertEquals(representationLatest, representation);
    }


    @Betamax(tape = "records/shouldTreatLatestPersistentVersionAsLatestCreated")
    @Test
    public void shouldTreatLatestPersistentVersionAsLatestCreated()
            throws MCSException, IOException {
        String providerId = "Provider001";
        String cloudId = "J93T5R6615H";
        String schema = "schema33";
        String fileType = "text/plain";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        FileServiceClient fileService = new FileServiceClient(baseUrl);

        //create representation A
        URI uriA = instance.createRepresentation(cloudId, schema, providerId);
        //create representation B
        URI uriB = instance.createRepresentation(cloudId, schema, providerId);
        //obtain version codes
        String versionA = TestUtils.obtainRepresentationFromURI(instance, uriA).getVersion();
        String versionB = TestUtils.obtainRepresentationFromURI(instance, uriB).getVersion();
        //add files
        fileService.uploadFile(cloudId, schema, versionA, new ByteArrayInputStream("fileA".getBytes()), fileType);
        fileService.uploadFile(cloudId, schema, versionB, new ByteArrayInputStream("fileB".getBytes()), fileType);
        //persist representation B
        instance.persistRepresentation(cloudId, schema, versionB);
        //persist representation A
        instance.persistRepresentation(cloudId, schema, versionA);
        //check what was obtained
        Representation representation = instance.getRepresentation(cloudId, schema);
        assertEquals(representation.getVersion(), versionB);
    }


    @Betamax(tape = "records/getRepresentationVersionNoRecord")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationVersionWhenNoRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        String schema = "schema22";
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentation(cloudId, schema, version);
    }


    @Betamax(tape = "records/getRepresentationVersionNoSchema")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationVersionWhenNoSchema()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String schema = "noSuchSchema";
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentation(cloudId, schema, version);
    }


    @Betamax(tape = "records/getRepresentationVersionNoSuchVersion")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForGetRepresentationVersionWhenNoSuchVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String schema = "schema22";
        //there is no such version, but the UUID is valid
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6013";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentation(cloudId, schema, version);
    }


    @Betamax(tape = "records/getRepresentationVersionInvalidVersion")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForGetRepresentationVersionWhenInvalidVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String schema = "schema22";
        //there is no such version and the UUID is invalid
        String version = "noSuchVersion";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentation(cloudId, schema, version);
    }


    //for example when Cassandra is not working
    @Betamax(tape = "records/getRepresentationVersionInternalServerError")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForGetRepresentationVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String schema = "schema22";
        String version = "6e2b47e0-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentation(cloudId, schema, version);
    }


    //deleteRepresentation(cloudId, schema, version)
    @Betamax(tape = "records/deleteRepresentationVersionSuccess")
    @Test
    public void shouldDeleteRepresentationVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String schema = "schema22";
        String version = "6e2b47e0-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, schema, version);

        //try to get this version
        Boolean noVersion = false;
        try {
            instance.getRepresentation(cloudId, schema, version);
        } catch (RepresentationNotExistsException ex) {
            noVersion = true;
        }
        assertTrue(noVersion);
    }


    @Betamax(tape = "records/deleteRepresentationVersionNoRecord")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForDeleteRepresentationVersionWhenNoRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        String schema = "schema22";
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, schema, version);
    }


    @Betamax(tape = "records/deleteRepresentationVersionNoSchema")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForDeleteRepresentationVersionWhenNoSchema()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String schema = "noSuchSchema";
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, schema, version);
    }


    @Betamax(tape = "records/deleteRepresentationVersionNoSuchVersion")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForDeleteRepresentationVersionWhenNoSuchVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String schema = "schema22";
        //there is no such version, but the UUID is valid
        String version = "74cc8410-a2d9-11e3-8a55-1c6f653f6013";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, schema, version);
    }


    @Betamax(tape = "records/deleteRepresentationVersionInvalidVersion")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForDeleteRepresentationVersionWhenInvalidVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String schema = "schema22";
        //there is no such version and the UUID is invalid
        String version = "noSuchVersion";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, schema, version);
    }


    //for example when Cassandra is not working
    @Betamax(tape = "records/deleteRepresentationVersionInternalServerError")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForDeleteRepresentationVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String schema = "schema22";
        String version = "7b2349c0-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, schema, version);
    }


    @Betamax(tape = "records/deleteRepresentationVersionCannotModifyPersistent")
    @Test(expected = CannotModifyPersistentRepresentationException.class)
    public void shouldNotAllowToDeletePersistenRepresentation()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String schema = "schema22";
        //this version is persistent (but not latest - not important)
        String version = "67565180-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        //check this representation is persistent
        Representation representation = instance.getRepresentation(cloudId, schema, version);
        assertNotNull(representation);
        assertEquals(cloudId, representation.getCloudId());
        assertEquals(schema, representation.getRepresentationName());
        assertEquals(version, representation.getVersion());
        assertTrue(representation.isPersistent());

        //try to delete
        instance.deleteRepresentation(cloudId, schema, version);
    }


    //copyRepresentation
    @Betamax(tape = "records/shouldCopyNonPersistentRepresentation")
    @Test
    public void shouldCopyNonPersistentRepresentation()
            throws MCSException, IOException {

        String cloudId = "J93T5R6615H";
        String schema = "schema22";
        //not persistent representation with two files in it
        String sourceVersion = "8e93ef30-a2ef-11e3-89f5-1c6f653f6012";
        //I fix it because if I just checked that they have the same number it could be 0
        int filesSize = 2;
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        //make a copy
        URI targetURI = instance.copyRepresentation(cloudId, schema, sourceVersion);
        assertNotNull(targetURI);

        //get copying result
        Representation targetRepresentation = TestUtils.obtainRepresentationFromURI(instance, targetURI);
        //check that is has two files in it
        assertEquals(targetRepresentation.getFiles().size(), filesSize);
        //get the source version
        Representation sourceRepresentation = instance.getRepresentation(cloudId, schema, sourceVersion);
        //check the versions differ
        assertNotEquals(targetRepresentation.getVersion(), sourceRepresentation.getVersion());
        //check both versions are not persistent
        assertEquals(sourceRepresentation.isPersistent(), false);
        assertEquals(targetRepresentation.isPersistent(), false);
        //check that files content does not differ
        TestUtils.assertSameFiles(targetRepresentation, sourceRepresentation);

    }


    @Betamax(tape = "records/shouldCopyPersistentRepresentation")
    @Test
    public void shouldCopyPersistentRepresentation()
            throws MCSException, IOException {
        String cloudId = "J93T5R6615H";
        String schema = "schema22";
        //this is some persistent version with two files in it
        String sourceVersion = "88edb4d0-a2ef-11e3-89f5-1c6f653f6012";
        //I fix it because if I just checked that they have the same number it could be 0
        int filesSize = 2;
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        //make a copy
        URI targetURI = instance.copyRepresentation(cloudId, schema, sourceVersion);

        //check URI
        assertNotNull(targetURI);
        //get copying result
        Representation targetRepresentation = TestUtils.obtainRepresentationFromURI(instance, targetURI);
        //check that is has two files in it
        assertEquals(targetRepresentation.getFiles().size(), filesSize);
        //get the source version
        Representation sourceRepresentation = instance.getRepresentation(cloudId, schema, sourceVersion);
        //check the versions differ
        assertNotEquals(targetRepresentation.getVersion(), sourceRepresentation.getVersion());
        //check the source is persistent and target not
        assertEquals(sourceRepresentation.isPersistent(), true);
        assertEquals(targetRepresentation.isPersistent(), false);
        //check that files content does not differ
        TestUtils.assertSameFiles(targetRepresentation, sourceRepresentation);
    }


    @Betamax(tape = "records/shouldThrowRepresentationNotExistsForCopyRepresentationWhenNoRecord")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForCopyRepresentationWhenNoRecord()
            throws MCSException {
        String cloudId = "noSuchRecord";
        String schema = "schema22";
        String version = "88edb4d0-a2ef-11e3-89f5-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.copyRepresentation(cloudId, schema, version);
    }


    @Betamax(tape = "records/shouldThrowRepresentationNotExistsForCopyRepresentationWhenNoSchema")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForCopyRepresentationWhenNoSchema()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String schema = "noSuchSchema";
        String version = "88edb4d0-a2ef-11e3-89f5-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.copyRepresentation(cloudId, schema, version);
    }


    @Betamax(tape = "records/shouldThrowRepresentationNotExistsForCopyRepresentationVersionWhenNoSuchVersion")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForCopyRepresentationVersionWhenNoSuchVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String schema = "schema22";
        //there is no such version, but the UUID is valid
        String version = "88edb4d0-a2ef-11e3-89f5-1c6f653f6013";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.copyRepresentation(cloudId, schema, version);
    }


    @Betamax(tape = "records/shouldThrowDriverExceptionForCopyRepresentationVersionWhenInvalidVersion")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForCopyRepresentationVersionWhenInvalidVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String schema = "schema22";
        //there is no such version and the UUID is invalid
        String version = "noSuchVersion";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.copyRepresentation(cloudId, schema, version);
    }


    //for example when Cassandra is not working
    @Betamax(tape = "records/shouldThrowDriverExceptionForCopyRepresentationVersion")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForCopyRepresentationVersion()
            throws MCSException {
        String cloudId = "J93T5R6615H";
        String schema = "schema22";
        String version = "7b2349c0-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.copyRepresentation(cloudId, schema, version);
    }


    //persistRepresentation
    //@Betamax(tape = "records/persistRepresentationSuccess") //TODO record when uploading is ok
    @Ignore
    @Test
    public void persistRepresentation()
            throws MCSException, IOException {
        String providerId = "Provider001";
        String cloudId = "4H7HC9HT46F";
        String schema = "schema1";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        String fileContent = "The content of the file.";
        String fileType = "text/plain";
        InputStream data = new ByteArrayInputStream(fileContent.getBytes());
        FileServiceClient fileService = new FileServiceClient(baseUrl);

        //create new representation version
        URI uri = instance.createRepresentation(cloudId, schema, providerId);
        //obtain the version
        String version = TestUtils.parseRepresentationFromUri(uri).getVersion();
        //add files to it
        //TODO
        try {
            fileService.uploadFile(cloudId, schema, version, data, fileType);
        } catch (Exception ex) {
        }
        //persist
        URI uriResult = instance.persistRepresentation(cloudId, schema, version);
        assertNotNull(uriResult);
        assertEquals(uri.toString(), uriResult.toString());
    }


    //TODO
    @Ignore
    @Test
    public void shouldPersistAfterAddingFiles()
            throws MCSException, IOException {
        String cloudId = "J93T5R6615H";
        String schema = "schema22";
        String version = "67565180-a2d9-11e3-8a55-1c6f653f6012";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        FileServiceClient fileService = new FileServiceClient(baseUrl);
        String fileContent = "The content of the file.";
        String fileType = "text/plain";
        InputStream data = new ByteArrayInputStream(fileContent.getBytes());

        try {
            fileService.uploadFile(cloudId, schema, version, data, fileType);
        } catch (Exception e) {
        }

        URI uriResult = instance.persistRepresentation(cloudId, schema, version);
        assertNotNull(uriResult);
    }

}
