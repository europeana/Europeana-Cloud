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
import java.net.URI;
import java.util.List;
import org.junit.Rule;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Ignore;
import org.junit.Test;

public class RecordServiceClientTest {

    @Rule
    public Recorder recorder = new Recorder();

    //TODO clean
    //this is only needed for recording tests
    private final String baseUrl = "http://localhost:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/";

    //TODO TU - review all tests, names and checked parameters
    @Betamax(tape = "records/getRecord_cloudId_Successfully")
    @Test
    public void getRecord_cloudId_Successfully()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        Record record = instance.getRecord(cloudId);
        assertNotNull(record);
        assertEquals(cloudId, record.getId());
    }

    @Betamax(tape = "records/getRecord_cloudId_404")
    @Test(expected = RecordNotExistsException.class)
    public void getRecord_cloudId_404()
            throws MCSException {
        String cloudId = "7MZWQJF8P84_";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRecord(cloudId);
    }

    @Betamax(tape = "records/deleteRecord_cloudId_Successfully")
    @Test
    public void _deleteRecord_cloudId_Successfully()
            throws MCSException {
        String cloudId = "3SWK3L9K179";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRecord(cloudId);
    }

    @Ignore("Error in api")
    @Test(expected = RecordNotExistsException.class)
    public void _deleteRecord_cloudId_TwoTimes()
            throws MCSException {
        String cloudId = "RK4F1HPD6RH";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        instance.deleteRecord(cloudId);
        instance.deleteRecord(cloudId);
    }

    @Betamax(tape = "records/getRepresentations_cloudId_Successfully")
    @Test
    public void getRepresentations_cloudId_Successfully()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        List<Representation> representationList = instance.getRepresentations(cloudId);
        assertNotNull(representationList);
        for (Representation representation : representationList) {
            assertEquals(cloudId, representation.getRecordId());
        }
    }

    @Betamax(tape = "records/getRepresentations_cloudId_404")
    @Test(expected = RecordNotExistsException.class)
    public void getRepresentations_cloudId_404()
            throws MCSException {
        String cloudId = "7MZWQJF8P84_";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        List<Representation> representationList = instance.getRepresentations(cloudId);
        assertNotNull(representationList);
        for (Representation representation : representationList) {
            assertEquals(cloudId, representation.getRecordId());
        }
    }

    @Betamax(tape = "records/getRepresentation_cloudId_schema_Successfully")
    @Test
    public void getRepresentation_cloudId_schema_Successfully()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        String schema = "schema_000001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        Representation representation = instance.getRepresentation(cloudId, schema);
        assertNotNull(representation);
        assertEquals(cloudId, representation.getRecordId());
        assertEquals(schema, representation.getSchema());
    }

    @Ignore("In api is specyfied RecordNotExistException but is thrown RepresentationNotExistException")
    @Test(expected = RecordNotExistsException.class)
    public void getRepresentation_cloudId_schema_404()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        String schema = "schema_000001_";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        Representation representation = instance.getRepresentation(cloudId, schema);
    }

    private void assertCorrectlyCreatedRepresentation(RecordServiceClient instance, URI uri, String providerId, String cloudId, String schema) throws MCSException {

        String[] elements = uri.getRawPath().split("/");
        String uriVersion = elements[elements.length - 1];
        String uriSchema = elements[elements.length - 3];
        String uriRecordId = elements[elements.length - 5];
        assertEquals(cloudId, uriRecordId);
        assertEquals(schema, uriSchema);

        //get representation and check
        Representation representation = instance.getRepresentation(cloudId, schema, uriVersion);
        assertNotNull(representation);
        assertEquals(cloudId, representation.getRecordId());
        assertEquals(schema, representation.getSchema());
        assertEquals(providerId, representation.getDataProvider());
        assertEquals(uriVersion, representation.getVersion());
    }

    @Betamax(tape = "records/createRepresentationSuccess")
    @Test
    public void shouldCreateRepresentation()
            throws MCSException {

        String cloudId = "1DZ6HTS415W";
        String schema = "schema77";
        String providerId = "Provider001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        URI uri = instance.createRepresentation(cloudId, schema, providerId);
        assertNotNull(uri);

        assertCorrectlyCreatedRepresentation(instance, uri, providerId, cloudId, schema);

    }

    //@Betamax(tape = "records/createRepresentationRecordNotExists")
    @Test(expected = RecordNotExistsException.class)
    @Ignore("TODO")
    public void shouldThrowRecordNotExistsForCreateRepresentation()
            throws MCSException {

        String cloudId = "1DZ6HTS415W";
        String schema = "schema_000001";
        String providerId = "Provider001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.createRepresentation(cloudId, schema, providerId);
    }

    @Betamax(tape = "records/createRepresentationSchemaNotExists")
    @Test
    public void shouldCreateNewSchemaWhenNotExists()
            throws MCSException {

        String cloudId = "1DZ6HTS415W";
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
        //assertTrue(schemaNotExists); TODO can be restored when fixed ECL-140, tape will have to be rerecorded

        URI uri = instance.createRepresentation(cloudId, schema, providerId);
        assertCorrectlyCreatedRepresentation(instance, uri, providerId, cloudId, schema);
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

    @Betamax(tape = "records/deletesRepresentation_cloudId_schema_Successfully")
    @Test
    public void deletesRepresentation_cloudId_schema_Successfully()
            throws MCSException {
        String cloudId = "1TZC17H34S5";
        String schema = "schema_000001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deletesRepresentation(cloudId, schema);
    }

    @Ignore("Error in api")
    @Test(expected = RepresentationNotExistsException.class)
    public void _deletesRepresentation_cloudId_schema_TwoTimes()
            throws MCSException {
        String cloudId = "1TZC17H34S5";
        String schema = "schema_000001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deletesRepresentation(cloudId, schema);
    }

    @Ignore("Ask")
    @Test(expected = RepresentationNotExistsException.class)
    public void deletesRepresentation_cloudId_schema_404_incorrectId()
            throws MCSException {
        String cloudId = "1TZC17H34S5_";
        String schema = "schema_000001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deletesRepresentation(cloudId, schema);
    }

    @Ignore("Ask")
    @Test(expected = RepresentationNotExistsException.class)
    public void deletesRepresentation_cloudId_schema_404_incorrectSchema()
            throws MCSException {
        String cloudId = "1TZC17H34S5";
        String schema = "schema_000001_";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deletesRepresentation(cloudId, schema);
    }

    @Betamax(tape = "records/getRepresentations_cloudId_schema_Successfully")
    @Test
    public void getRepresentations_cloudId_schema_Successfully()
            throws RepresentationNotExistsException, MCSException {
        String cloudId = "7MZWQJF8P84";
        String schema = "schema_000001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        List<Representation> resultRepresentationList = instance.getRepresentations(cloudId, schema);
        assertNotNull(resultRepresentationList);
        for (Representation representation : resultRepresentationList) {
            assertEquals(cloudId, representation.getRecordId());
            assertEquals(schema, representation.getSchema());
        }
    }

    @Ignore("Ask")
    @Test(expected = RepresentationNotExistsException.class)
    public void getRepresentations_cloudId_schema_404_incorrectId()
            throws MCSException {
        String cloudId = "7MZWQJF8P84_";
        String schema = "schema_000001";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        List<Representation> resultRepresentationList = instance.getRepresentations(cloudId, schema);
    }

    @Ignore("Ask")
    @Test(expected = RepresentationNotExistsException.class)
    public void getRepresentations_cloudId_schema_404_incorrectSchema()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        String schema = "schema_000001x";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        List<Representation> resultRepresentationList = instance.getRepresentations(cloudId, schema);
    }

    @Betamax(tape = "records/getRepresentation_cloudID_schema_version_Successfully")
    @Test
    public void getRepresentation_cloudID_schema_version_Successfully()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        String schema = "schema_000001";
        String version = "3480fe50-9888-11e3-b072-50e549e85271";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        Representation representation = instance.getRepresentation(cloudId, schema, version);
        assertNotNull(representation);
        assertEquals(cloudId, representation.getRecordId());
        assertEquals(schema, representation.getSchema());
        assertEquals(version, representation.getVersion());
    }

    @Betamax(tape = "records/getRepresentation_cloudID_schema_version_Latest")
    @Test
    public void getRepresentation_cloudID_schema_version_Latest()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        String schema = "schema_000001";
        String version = "LATEST";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        Representation representation = instance.getRepresentation(cloudId, schema, version);
        assertNotNull(representation);
        assertEquals(cloudId, representation.getRecordId());
        assertEquals(schema, representation.getSchema());
    }

    @Betamax(tape = "records/getRepresentation_cloudID_schema_version_incorrectId")
    @Test(expected = RepresentationNotExistsException.class)
    public void getRepresentation_cloudID_schema_version_incorrectId()
            throws MCSException {
        String cloudId = "7MZWQJF8P84_";
        String schema = "schema_000001";
        String version = "71cdeb90-955b-11e3-b6af-50e549e85271";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentation(cloudId, schema, version);
    }

    @Betamax(tape = "records/getRepresentation_cloudID_schema_version_incorrectSchema")
    @Test(expected = RepresentationNotExistsException.class)
    public void getRepresentation_cloudID_schema_version_incorrectSchema()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        String schema = "schema_000001_";
        String version = "71cdeb90-955b-11e3-b6af-50e549e85271";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentation(cloudId, schema, version);
    }

    @Ignore("Ask")
    @Test(expected = RepresentationNotExistsException.class)
    public void getRepresentation_cloudID_schema_version_incorrectVersion()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        String schema = "schema_000001";
        String version = "71cdeb90-955b-11e3-b6af-50e549e85271_";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.getRepresentation(cloudId, schema, version);
    }

    @Betamax(tape = "records/deleteRepresentation_cloudID_schema_version_Successfully")
    @Test
    public void deleteRepresentation_cloudID_schema_version_Successfully()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        String schema = "schema_000001";
        String version = "5dfded60-988d-11e3-b072-50e549e85271";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, schema, version);
    }

    @Betamax(tape = "records/deleteRepresentation_cloudID_schema_version_incorrectId")
    @Test(expected = RepresentationNotExistsException.class)
    public void deleteRepresentation_cloudID_schema_version_incorrectId()
            throws MCSException {
        String cloudId = "7MZWQJF8P84_";
        String schema = "schema_000001";
        String version = "ae7dd340-97c5-11e3-b4e8-50e549e85271";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, schema, version);
    }

    @Betamax(tape = "records/deleteRepresentation_cloudID_schema_version_incorrectSchema")
    @Test(expected = RepresentationNotExistsException.class)
    public void deleteRepresentation_cloudID_schema_version_incorrectSchema()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        String schema = "schema_000001_";
        String version = "ae7dd340-97c5-11e3-b4e8-50e549e85271";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, schema, version);
    }

    @Ignore("Now is trow InternalError but should be RepresentationNotExistsException")
    @Test(expected = RepresentationNotExistsException.class)
    public void deleteRepresentation_cloudID_schema_version_incorrectVersion()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        String schema = "schema_000001";
        String version = "89feff50-94ad-11e3-ac19-50e549e85271_";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, schema, version);
    }

    @Ignore("Wron exeption is throw.")
    @Test(expected = CannotModifyPersistentRepresentationException.class)
    public void deleteRepresentation_cloudID_schema_version_persisted()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        String schema = "schema_000001";
        String version = "a36c2120-8e4f-11e3-81d2-50e549e85271";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.deleteRepresentation(cloudId, schema, version);
    }

    @Betamax(tape = "records/copyRepresentation_Successfully")
    @Test
    public void copyRepresentation_Successfully()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        String schema = "schema_000001";
        String version = "3480fe50-9888-11e3-b072-50e549e85271";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);
        URI uriResult = instance.copyRepresentation(cloudId, schema, version);
        assertNotNull(uriResult);
    }

    @Betamax(tape = "records/copyRepresentation_incorrectId")
    @Test(expected = RepresentationNotExistsException.class)
    public void copyRepresentation_incorrectId()
            throws MCSException {
        String cloudId = "7MZWQJF8P84_";
        String schema = "schema_000001";
        String version = "ff67be10-9888-11e3-b072-50e549e85271";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.copyRepresentation(cloudId, schema, version);
    }

    @Betamax(tape = "records/copyRepresentation_incorrectSchema")
    @Test(expected = RepresentationNotExistsException.class)
    public void copyRepresentation_incorrectSchema()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        String schema = "schema_000001_";
        String version = "e9261830-955a-11e3-b6af-50e549e85271";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.copyRepresentation(cloudId, schema, version);
    }

    @Ignore("Test fault is DriverException")
    @Test(expected = RepresentationNotExistsException.class)
    public void copyRepresentation_incorrectVersion()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        String schema = "schema_000001";
        String version = "e9261830-955a-11e3-b6af-50e549e85271_";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        instance.copyRepresentation(cloudId, schema, version);
    }

    @Betamax(tape = "records/persistRepresentation_Successfully")
    @Test
    public void persistRepresentation_Successfully()
            throws MCSException {
        String cloudId = "7MZWQJF8P84";
        String schema = "schema_000001";
        String version = "ff67be10-9888-11e3-b072-50e549e85271";
        RecordServiceClient instance = new RecordServiceClient(baseUrl);

        URI uriResult = instance.copyRepresentation(cloudId, schema, version);
        assertNotNull(uriResult);
    }

}
