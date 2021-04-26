package eu.europeana.cloud.mcs.driver;

import co.freeside.betamax.Betamax;
import co.freeside.betamax.Recorder;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.CloudVersionRevisionResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.*;
import org.junit.Rule;
import org.junit.Test;

import java.net.URI;
import java.util.List;
import java.util.NoSuchElementException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.*;

public class DataSetServiceClientTest {

    @Rule
    public Recorder recorder = new Recorder();

    //TODO clean
    //this is only needed for recording tests
    private final String baseUrl = "http://localhost:8080/mcs";

    @Betamax(tape = "dataSets_shouldRetrieveDataSetsFirstChunk")
    @Test
    public void shouldRetrieveDataSetsFirstChunk()
            throws MCSException {
        String providerId = "Provider002";
        //the tape was recorded when the result chunk was 100
        int resultSize = 100;
        String startFrom = "dataset000101";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        ResultSlice<DataSet> result = instance.getDataSetsForProviderChunk(providerId, null);
        assertNotNull(result.getResults());
        assertEquals(result.getResults().size(), resultSize);
        assertEquals(result.getNextSlice(), startFrom);
    }

    @Betamax(tape = "dataSets_shouldRetrieveDataSetsSecondChunk")
    @Test
    public void shouldRetrieveDataSetsSecondChunk()
            throws MCSException {
        String providerId = "Provider002";
        int resultSize = 100;
        String startFrom = "dataset000101";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        ResultSlice<DataSet> result = instance.getDataSetsForProviderChunk(providerId, startFrom);
        assertNotNull(result.getResults());
        assertEquals(result.getResults().size(), resultSize);
        assertNull(result.getNextSlice());
    }

    @Betamax(tape = "dataSets_shouldNotThrowProviderNotExistsForDataSetsChunk")
    @Test
    public void shouldNotThrowProviderNotExistsForDataSetsChunk()
            throws MCSException {
        String providerId = "notFoundProviderId";
        String startFrom = null;

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        ResultSlice<DataSet> result = instance.getDataSetsForProviderChunk(providerId, startFrom);
        assertNotNull(result.getResults());
        assertEquals(0, result.getResults().size());
    }

    @Betamax(tape = "dataSets_shouldReturnAllDataSets")
    @Test
    public void shouldReturnAllDataSets()
            throws MCSException {
        String providerId = "Provider002";
        int resultSize = 200;

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        List<DataSet> result = instance.getDataSetsForProvider(providerId);
        assertNotNull(result);
        assertEquals(result.size(), resultSize);
    }

    @Betamax(tape = "dataSets_shouldNotThrowProviderNotExistsForDataSetsAll")
    @Test
    public void shouldNotThrowProviderNotExistsForDataSetsAll()
            throws MCSException {
        String providerId = "notFoundProviderId";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        List<DataSet> result = instance.getDataSetsForProvider(providerId);
        assertNotNull(result);
        assertEquals(0, result.size());
    }

    //to test it you can turn off Cassandra
    @Betamax(tape = "dataSets_shouldThrowDriverExceptionForGetDataSetsChunk")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForGetDataSetsChunk()
            throws MCSException {
        String providerId = "Provider001";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.getDataSetsForProviderChunk(providerId, null);
    }

    @Betamax(tape = "dataSets_shouldThrowDriverExceptionForGetDataSets")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForGetDataSets()
            throws MCSException {
        String providerId = "Provider001";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.getDataSetsForProviderChunk(providerId, null);
    }

    @Betamax(tape = "dataSets_shouldSuccessfullyCreateDataSet")
    @Test
    public void shouldSuccessfullyCreateDataSet()
            throws MCSException {
        String providerId = "Provider001";
        String dataSetId = "dataset000008";
        String description = "description01";

        String expectedLocation = baseUrl + "/data-providers/" + providerId + "/data-sets/" + dataSetId;

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        URI result = instance.createDataSet(providerId, dataSetId, description);
        assertEquals(result.toString(), expectedLocation);
    }

    @Betamax(tape = "dataSets_shouldThrowDataSetAlreadyExists")
    @Test(expected = DataSetAlreadyExistsException.class)
    public void shouldThrowDataSetAlreadyExists()
            throws MCSException {
        String providerId = "Provider001";
        String dataSetId = "dataset000002";
        String description = "description";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.createDataSet(providerId, dataSetId, description);
    }

    @Betamax(tape = "dataSets_shouldThrowProviderNotExists")
    @Test(expected = ProviderNotExistsException.class)
    public void shouldThrowProviderNotExists()
            throws MCSException {
        String providerId = "notFoundProviderId";
        String dataSetId = "dataSetId";
        String description = "description";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.createDataSet(providerId, dataSetId, description);
    }

    @Betamax(tape = "dataSets_shouldThrowDriverExceptionForCreateDataSet")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForCreateDataSet()
            throws MCSException {
        String providerId = "providerId";
        String dataSetId = "dataSetId";
        String description = "description";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.createDataSet(providerId, dataSetId, description);
    }

    @Betamax(tape = "dataSets_shouldRetrieveRepresentationsFirstChunk")
    @Test
    public void shouldRetrieveRepresentationsFirstChunk()
            throws MCSException {
        String providerId = "Provider001";
        String dataSetId = "dataset000002";
        //the tape was recorded when the result chunk was 100
        int resultSize = 100;
        String startFrom = "G5DFUSCILJFVGQSEJYFHGY3IMVWWCMI=";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        ResultSlice<Representation> result = instance.getDataSetRepresentationsChunk(providerId, dataSetId, null);
        assertNotNull(result.getResults());
        assertEquals(result.getResults().size(), resultSize);
        assertEquals(result.getNextSlice(), startFrom);
    }

    @Betamax(tape = "dataSets_shouldRetrieveRepresentationsSecondChunk")
    @Test
    public void shouldRetrieveRepresentationsSecondChunk()
            throws MCSException {
        String providerId = "Provider001";
        String dataSetId = "dataset000002";
        int resultSize = 100;
        String startFrom = "G5DFUSCILJFVGQSEJYFHGY3IMVWWCMI=";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        ResultSlice<Representation> result = instance.getDataSetRepresentationsChunk(providerId, dataSetId, startFrom);
        assertNotNull(result.getResults());
        assertEquals(result.getResults().size(), resultSize);
        assertNull(result.getNextSlice());
    }

    @Betamax(tape = "dataSets_shouldThrowDataSetNotExistsForRepresentationsChunk")
    @Test(expected = DataSetNotExistsException.class)
    public void shouldThrowDataSetNotExistsForRepresentationsChunk()
            throws MCSException {
        String providerId = "Provider001";
        String dataSetId = "dataset000042";
        String startFrom = "G5DFUSCILJFVGQSEJYFHGY3IMVWWCMI=";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.getDataSetRepresentationsChunk(providerId, dataSetId, startFrom);
    }

    @Betamax(tape = "dataSets_shouldReturnAllRepresentations")
    @Test
    public void shouldReturnAllRepresentations()
            throws MCSException {
        String providerId = "Provider001";
        String dataSetId = "dataset000002";
        int resultSize = 200;

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        List<Representation> result = instance.getDataSetRepresentations(providerId, dataSetId);
        assertNotNull(result);
        assertEquals(result.size(), resultSize);
    }

    @Betamax(tape = "dataSets_shouldThrowDataSetNotExistsForRepresentationsAll")
    @Test(expected = DataSetNotExistsException.class)
    public void shouldThrowDataSetNotExistsForRepresentationsAll()
            throws MCSException {
        String providerId = "Provider001";
        String dataSetId = "dataset000042";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.getDataSetRepresentations(providerId, dataSetId);
    }

    @Betamax(tape = "dataSets_shouldThrowDriverExceptionForGetRepresentationsChunk")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForGetRepresentationsChunk()
            throws MCSException {
        String providerId = "Provider001";
        String dataSetId = "dataset000002";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.getDataSetRepresentationsChunk(providerId, dataSetId, null);
    }

    @Betamax(tape = "dataSets_shouldThrowDriverExceptionForGetRepresentations")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForGetRepresentations()
            throws MCSException {
        String providerId = "Provider001";
        String dataSetId = "dataset000002";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.getDataSetRepresentations(providerId, dataSetId);
    }

    //we cannot mock system state change in Betamax
    //because it will not record two different answers for the same request 
    @Betamax(tape = "dataSets_shouldUpdateDescriptionOfDataSet")
    @Test
    public void shouldUpdateDescriptionOfDataSet()
            throws MCSException {
        String providerId = "Provider002";
        String dataSetId = "dataset000002";
        String description = "TEST1";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);

        instance.updateDescriptionOfDataSet(providerId, dataSetId, description);
        List<DataSet> dataSets = instance.getDataSetsForProvider(providerId);

        for (DataSet dataSet : dataSets) {
            if (dataSetId.equals(dataSet.getId())) {

                assertEquals(dataSet.getDescription(), description);
            }

        }

    }

    @Betamax(tape = "dataSets_shouldUpdateDescriptionOfDataSetToEmpty")
    @Test
    public void shouldUpdateDescriptionOfDataSetToEmpty()
            throws MCSException {
        String providerId = "Provider002";
        String dataSetId = "dataset000002";
        String description = "";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);

        instance.updateDescriptionOfDataSet(providerId, dataSetId, description);
        List<DataSet> dataSets = instance.getDataSetsForProvider(providerId);

        for (DataSet dataSet : dataSets) {
            if (dataSetId.equals(dataSet.getId())) {

                assertEquals(dataSet.getDescription(), description);
            }

        }

    }

    @Betamax(tape = "dataSets_shouldUpdateDescriptionOfDataSetToNull")
    @Test
    public void shouldUpdateDescriptionOfDataSetToNull()
            throws MCSException {
        String providerId = "Provider002";
        String dataSetId = "dataset000002";
        String description = null;

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);

        instance.updateDescriptionOfDataSet(providerId, dataSetId, description);
        List<DataSet> dataSets = instance.getDataSetsForProvider(providerId);

        for (DataSet dataSet : dataSets) {
            if (dataSetId.equals(dataSet.getId())) {

                assertNull(dataSet.getDescription());
            }

        }

    }

    @Betamax(tape = "dataSets_shouldThrowDataSetNotExistsForUpdateDescription")
    @Test(expected = DataSetNotExistsException.class)
    public void shouldThrowDataSetNotExistsForUpdateDescription()
            throws MCSException {
        String providerId = "Provider002";
        String dataSetId = "noSuchDataset";
        String description = "TEST4";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.updateDescriptionOfDataSet(providerId, dataSetId, description);
    }

    @Betamax(tape = "dataSets_shouldThrowDriverExceptionForUpdateDescription")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForUpdateDescription()
            throws MCSException {
        String providerId = "Provider002";
        String dataSetId = "dataset000001";
        String description = "TEST3";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.updateDescriptionOfDataSet(providerId, dataSetId, description);
    }

    @Betamax(tape = "dataSets_shouldDeleteDataSet")
    @Test
    public void shouldDeleteDataSet()
            throws MCSException {
        String providerId = "Provider002";
        String dataSetId = "dataset000033";
        DataSet dataSet = new DataSet();
        dataSet.setProviderId(providerId);
        dataSet.setId(dataSetId);
        dataSet.setDescription(null);

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.deleteDataSet(providerId, dataSetId);

        List<DataSet> dataSets = instance.getDataSetsForProvider(providerId);

        assertFalse(dataSets.contains(dataSet));
    }

    @Betamax(tape = "dataSets_shouldThrowDataSetNotExistsForDeleteDataSet")
    @Test(expected = DataSetNotExistsException.class)
    public void shouldThrowDataSetNotExistsForDeleteDataSet()
            throws MCSException {
        String providerId = "Provider002";
        String dataSetId = "dataset000033";
        DataSet dataSet = new DataSet();
        dataSet.setProviderId(providerId);
        dataSet.setId(dataSetId);
        dataSet.setDescription(null);

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.deleteDataSet(providerId, dataSetId);
    }

    @Betamax(tape = "dataSets_shouldThrowDriverExceptionForDeleteDataSet")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForDeleteDataSet()
            throws MCSException {
        String providerId = "Provider002";
        String dataSetId = "dataset000033";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.deleteDataSet(providerId, dataSetId);

    }

    @Betamax(tape = "dataSets_shouldAssignRepresentation")
    @Test
    public void shouldAssignRepresentation()
            throws MCSException {
        String providerId = "Provider002";
        String dataSetId = "dataset000008";
        String cloudId = "1DZ6HTS415W";
        String representationName = "schema66";
        //this is the last persistent version
        String versionId = "b95fcda0-994a-11e3-bfe1-1c6f653f6012";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.assignRepresentationToDataSet(providerId, dataSetId, cloudId, representationName, null);

        assertEquals(1, TestUtils.howManyThisRepresentationVersion(instance, providerId, dataSetId, representationName, versionId));
    }

    //should not complain about assigning the same representation version again
    //this test does not have sense using Betamax
    //but I wrote it just in case
    @Betamax(tape = "dataSets_shouldAssignTheSameRepresentation")
    @Test
    public void shouldAssignTheSameRepresentation()
            throws MCSException {

        shouldAssignRepresentation();
        shouldAssignRepresentation();
    }

    @Betamax(tape = "dataSets_shouldAssignRepresentationVersion")
    @Test
    public void shouldAssignRepresentationVersion()
            throws MCSException {
        String providerId = "Provider001";
        String dataSetId = "dataset000066";
        String cloudId = "1DZ6HTS415W";
        String representationName = "schema77";
        String versionId1 = "49398390-9a3f-11e3-9690-1c6f653f6012";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);

        instance.assignRepresentationToDataSet(providerId, dataSetId, cloudId, representationName, versionId1);
        assertEquals(1, TestUtils.howManyThisRepresentationVersion(instance, providerId, dataSetId, representationName, versionId1));

    }

    //this test does not have sense using Betamax
    //but I wrote it just in case
    @Betamax(tape = "dataSets_shouldOverrideAssignedRepresentationVersion")
    @Test
    public void shouldOverrideAssignedRepresentationVersion()
            throws MCSException {
        String providerId = "Provider001";
        String dataSetId = "dataset000066";
        String cloudId = "1DZ6HTS415W";
        String representationName = "schema77";
        String versionId2 = "97dd0b70-9a3f-11e3-9690-1c6f653f6012";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);

        instance.assignRepresentationToDataSet(providerId, dataSetId, cloudId, representationName, versionId2);
        assertEquals(1, TestUtils.howManyThisRepresentationVersion(instance, providerId, dataSetId, representationName, versionId2));

    }

    @Betamax(tape = "dataSets_shouldThrowDriverExceptionForAssingRepresentation")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForAssingRepresentation()
            throws MCSException {
        String providerId = "Provider001";
        String dataSetId = "dataset000015";
        String cloudId = "1DZ6HTS415W";
        String representationName = "schema66";
        String versionId = "b929f090-994a-11e3-bfe1-1c6f653f6012";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.assignRepresentationToDataSet(providerId, dataSetId, cloudId, representationName, versionId);

    }

    @Betamax(tape = "dataSets_shouldThrowRepresentationNotExistsForAssingRepresentation")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsForAssingRepresentation()
            throws MCSException {
        String providerId = "Provider001";
        String dataSetId = "dataset000016";
        String cloudId = "1DZ6HTS415W";
        String representationName = "noSuchSchema";
        String versionId = null;

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.assignRepresentationToDataSet(providerId, dataSetId, cloudId, representationName, versionId);

    }

    @Betamax(tape = "dataSets_shouldThrowDataSetNotExistsForAssingRepresentation")
    @Test(expected = DataSetNotExistsException.class)
    public void shouldThrowDataSetNotExistsForAssingRepresentation()
            throws MCSException {
        String providerId = "Provider001";
        String dataSetId = "noSuchDataSet";
        String cloudId = "1DZ6HTS415W";
        String representationName = "schema66";
        String versionId = null;

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.assignRepresentationToDataSet(providerId, dataSetId, cloudId, representationName, versionId);

    }

    @Betamax(tape = "dataSets_shouldUnassignRepresentation")
    @Test
    public void shouldUnassignRepresentation()
            throws MCSException {
        String providerId = "Provider002";
        String dataSetId = "dataset000002";
        String cloudId = "1DZ6HTS415W";
        String representationName = "schema66";
        String representationVersion = "66404040-0307-11e6-a5cb-0050568c62b8";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.unassignRepresentationFromDataSet(providerId, dataSetId, cloudId, representationName, representationVersion);

        assertEquals(0, TestUtils.howManyThisRepresentationVersion(instance, providerId, dataSetId, representationName, null));
    }

    //should not complain about unassigning not assigned representation
    @Betamax(tape = "dataSets_shouldUnassignNotAssignedRepresentation")
    @Test
    public void shouldUnassignNotAssignedRepresentation()
            throws MCSException {
        String providerId = "Provider002";
        String dataSetId = "dataset000002";
        String cloudId = "1DZ6HTS415W";
        String representationName = "schema66";
        String representationVersion = "66404040-0307-11e6-a5cb-0050568c62b8";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        assertEquals(0, TestUtils.howManyThisRepresentationVersion(instance, providerId, dataSetId, representationName, null));
        instance.unassignRepresentationFromDataSet(providerId, dataSetId, cloudId, representationName, representationVersion);

    }

    @Betamax(tape = "dataSets_shouldUnassignRepresentationWithVersion")
    @Test
    public void shouldUnassignRepresentationWithVersion()
            throws MCSException {
        String providerId = "Provider001";
        String dataSetId = "dataset000023";
        String cloudId = "1DZ6HTS415W";
        String representationName = "schema66";
        String representationVersion = "66404040-0307-11e6-a5cb-0050568c62b8";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.unassignRepresentationFromDataSet(providerId, dataSetId, cloudId, representationName, representationVersion);
        assertEquals(0, TestUtils.howManyThisRepresentationVersion(instance, providerId, dataSetId, representationName, null));

    }

    //should not complain about unassigning non-existing representation
    @Betamax(tape = "dataSets_shouldUnassignNonExistingRepresentation")
    @Test
    public void shouldUnassignNonExistingRepresentation()
            throws MCSException {
        String providerId = "Provider002";
        String dataSetId = "dataset000007";
        String cloudId = "1DZ6HTS415W";
        String representationName = "noSuchSchema";
        String representationVersion = "66404040-0307-11e6-a5cb-0050568c62b8";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.unassignRepresentationFromDataSet(providerId, dataSetId, cloudId, representationName, representationVersion);

        assertTrue(true);
    }

    @Betamax(tape = "dataSets_shouldThrowDriverExceptionForUnassingRepresentation")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForUnassingRepresentation()
            throws MCSException {
        String providerId = "Provider001";
        String dataSetId = "dataset000058";
        String cloudId = "1DZ6HTS415W";
        String representationName = "schema77";
        String representationVersion = "66404040-0307-11e6-a5cb-0050568c62b8";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.unassignRepresentationFromDataSet(providerId, dataSetId, cloudId, representationName, representationVersion);

    }

    @Betamax(tape = "dataSets_shouldThrowDataSetNotExistsForUnassingRepresentation")
    @Test(expected = DataSetNotExistsException.class)
    public void shouldThrowDataSetNotExistsForUnassingRepresentation()
            throws MCSException {
        String providerId = "Provider002";
        String dataSetId = "noSuchDataSet";
        String cloudId = "1DZ6HTS415W";
        String representationName = "schema77";
        String representationVersion = "66404040-0307-11e6-a5cb-0050568c62b8";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.unassignRepresentationFromDataSet(providerId, dataSetId, cloudId, representationName, representationVersion);

    }

    //data set iterator
    @Betamax(tape = "dataSets_shouldProvideDataSetIterator")
    @Test
    public void shouldProvideDataSetIterator()
            throws MCSException {
        String providerId = "Provider001";
        int numberOfDataSets = 200;
        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);

        DataSetIterator iterator = instance.getDataSetIteratorForProvider(providerId);
        assertNotNull(iterator);
        int counter = 0;
        while (iterator.hasNext()) {
            counter++;
            assertNotNull(iterator.next());
        }
        assertEquals(counter, numberOfDataSets);

    }

    @Betamax(tape = "dataSets_shouldProvideEmptyDataSetIteratorWhenNoSuchProvider")
    @Test
    public void shouldProvideEmptyDataSetIteratorWhenNoSuchProvider()
            throws MCSException {
        String providerId = "noSuchProvider";
        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);

        DataSetIterator iterator = instance.getDataSetIteratorForProvider(providerId);
        assertNotNull(iterator);
        assertEquals(false, iterator.hasNext());

    }

    @Betamax(tape = "dataSets_shouldProvideDataSetIteratorThatThrowsNoSuchElementException")
    @Test(expected = NoSuchElementException.class)
    public void shouldProvideDataSetIteratorThatThrowsNoSuchElementException()
            throws MCSException {
        String providerId = "Provider001";
        int numberOfDataSets = 200;
        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);

        DataSetIterator iterator = instance.getDataSetIteratorForProvider(providerId);
        assertNotNull(iterator);
        for (int i = 0; i < numberOfDataSets; i++) {
            //catch exception here, because it is not when we want it to be thrown
            try {
                assertNotNull(iterator.next());
            } catch (NoSuchElementException ex) {
                assert false : "NoSuchElementException thrown in unexpected place.";
            }
        }

        iterator.next();

    }

    @Betamax(tape = "dataSets_shouldProvideDataSetIteratorThatThrowsDriverException")
    @Test(expected = DriverException.class)
    public void shouldProvideDataSetIteratorThatThrowsDriverException()
            throws MCSException {
        String providerId = "Provider001";
        int numberOfDataSets = 200;
        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);

        DataSetIterator iterator = instance.getDataSetIteratorForProvider(providerId);
        iterator.next();
    }

    //representation iterator
    @Betamax(tape = "dataSets_shouldProvideRepresentationIterator")
    @Test
    public void shouldProvideRepresentationIterator()
            throws MCSException {
        String providerId = "Provider001";
        String dataSetId = "dataset3";
        int numberOfRepresentations = 200;
        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);

        RepresentationIterator iterator = instance.getRepresentationIterator(providerId, dataSetId);
        assertNotNull(iterator);
        int counter = 0;
        while (iterator.hasNext()) {
            counter++;
            assertNotNull(iterator.next());
        }
        assertEquals(counter, numberOfRepresentations);

    }

    @Betamax(tape = "dataSets_shouldProvideRepresentationIteratorThatThrowsDataSetNotExistsExceptionWhenNoDataSet")
    @Test(expected = DataSetNotExistsException.class)
    public void shouldProvideRepresentationIteratorThatThrowsExceptionWhenNoDataSet()
            throws Exception, Throwable {
        String providerId = "Provider001";
        String dataSetId = "noSuchDataSet";
        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);

        RepresentationIterator iterator = instance.getRepresentationIterator(providerId, dataSetId);
        assertNotNull(iterator);

        try {
            iterator.hasNext();
        } catch (DriverException e) {
            throw e.getCause();
        }
    }

    @Betamax(tape = "dataSets_shouldProvideRepresentationIteratorThatThrowsDataSetNotExistsExceptionWhenNoProvider")
    @Test(expected = DataSetNotExistsException.class)
    public void shouldProvideRepresentationIteratorThatThrowsExceptionWhenNoProvider()
            throws MCSException, Throwable {
        String providerId = "noSuchProvider";
        String dataSetId = "dataset3";
        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);

        RepresentationIterator iterator = instance.getRepresentationIterator(providerId, dataSetId);
        assertNotNull(iterator);

        try {
            iterator.hasNext();
        } catch (DriverException e) {
            throw e.getCause();
        }
    }

    @Betamax(tape = "dataSets_shouldProvideRepresentationIteratorThatThrowsNoSuchElementException")
    @Test(expected = NoSuchElementException.class)
    public void shouldProvideRepresentationIteratorThatThrowsNoSuchElementException()
            throws MCSException {
        String providerId = "Provider001";
        String dataSetId = "dataset3";
        int numberOfRepresentations = 200;
        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);

        RepresentationIterator iterator = instance.getRepresentationIterator(providerId, dataSetId);
        assertNotNull(iterator);
        for (int i = 0; i < numberOfRepresentations; i++) {
            //catch exception here, because it is not when we want it to be thrown
            try {
                assertNotNull(iterator.next());
            } catch (NoSuchElementException ex) {
                assert false : "NoSuchElementException thrown in unexpected place.";
            }
        }

        iterator.next();

    }

    @Betamax(tape = "dataSets_shouldProvideRepresentationIteratorThatThrowsDriverException")
    @Test(expected = DriverException.class)
    public void shouldProvideRepresentationIteratorThatThrowsDriverException()
            throws MCSException {
        String providerId = "Provider001";
        String dataSetId = "dataset3";
        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);

        RepresentationIterator iterator = instance.getRepresentationIterator(providerId, dataSetId);
        iterator.next();
    }

    @Betamax(tape = "dataSets_shouldRetrieveCloudIdsForSpecificRevision")
    @Test
    public void shouldRetrieveCloudIdsForSpecificRevision()
            throws MCSException {
        //given
        String providerId = "LFT";
        String dataSetId = "set1";
        String representationName = "t1";
        String revisionName = "IMPORT";
        String revisionProviderId = "EU";
        String revisionTimestamp = "2017-01-09T08:16:47.824";
        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        //when
        List<CloudTagsResponse> cloudIds = instance.getDataSetRevisions(providerId, dataSetId, representationName,
                revisionName, revisionProviderId, revisionTimestamp);
        //then
        assertThat(cloudIds.size(), is(2));
        CloudTagsResponse cid = cloudIds.get(0);
        assertThat(cid.getCloudId(), is("A2YCHGEFD4UV4UIEAWDUJHWJNZWXNOURWCQORIG7MCQASTB62OSQ"));
        assertTrue(cid.isAcceptance());
        assertFalse(cid.isDeleted());
        assertFalse(cid.isPublished());

        cid = cloudIds.get(1);
        assertThat(cid.getCloudId(), is("V7UYW5HK2YVQH7HN67W4ZRXBKLXLEY2HRIICIWAFTDVHEFZE5SPQ"));
        assertFalse(cid.isAcceptance());
        assertFalse(cid.isDeleted());
        assertTrue(cid.isPublished());
    }

    @Betamax(tape = "dataSets_shouldRetrievCloudIdsChunkForSpecificRevision")
    @Test
    public void shouldRetrievCloudIdsChunkForSpecificRevision()
            throws MCSException {
        //given
        String providerId = "LFT";
        String dataSetId = "set1";
        String representationName = "t1";
        String revisionName = "IMPORT";
        String revisionProviderId = "EU";
        String revisionTimestamp = "2017-01-09T08:16:47.824";
        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        //when
        ResultSlice<CloudTagsResponse> cloudIds = instance.getDataSetRevisionsChunk(providerId, dataSetId, representationName,
                revisionName, revisionProviderId, revisionTimestamp, null, null);
        //then
        assertThat(cloudIds.getNextSlice(), nullValue());
        assertThat(cloudIds.getResults().size(), is(2));
        CloudTagsResponse cid = cloudIds.getResults().get(0);
        assertThat(cid.getCloudId(), is("A2YCHGEFD4UV4UIEAWDUJHWJNZWXNOURWCQORIG7MCQASTB62OSQ"));
        assertTrue(cid.isAcceptance());
        assertFalse(cid.isDeleted());
        assertFalse(cid.isPublished());

        cid = cloudIds.getResults().get(1);
        assertThat(cid.getCloudId(), is("V7UYW5HK2YVQH7HN67W4ZRXBKLXLEY2HRIICIWAFTDVHEFZE5SPQ"));
        assertFalse(cid.isAcceptance());
        assertFalse(cid.isDeleted());
        assertTrue(cid.isPublished());
    }

}
