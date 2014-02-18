package eu.europeana.cloud.mcs.driver;

import co.freeside.betamax.Betamax;
import co.freeside.betamax.Recorder;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import java.net.URI;
import java.util.List;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import org.junit.Ignore;

public class DataSetServiceClientTest {

    @Rule
    public Recorder recorder = new Recorder();

    //TODO clean
    //this is only needed for recording tests
    private final String baseUrl = "http://localhost:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT";

    @Betamax(tape = "dataSets/getDataSetsChunkSuccess")
    @Test
    public void shouldRetrieveDataSetsFirstChunk()
            throws Exception {
        String providerId = "Provider002";
        //the tape was recorded when the result chunk was 100
        int resultSize = 100;
        String startFrom = "dataset000101";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        ResultSlice<DataSet> result = instance.getDataSetsForProviderChunk(providerId, null);
        assertNotNull(result.getResults());
        assertThat(result.getResults().size(), is(resultSize));
        assertThat(result.getNextSlice(), is(startFrom));
    }

    @Betamax(tape = "dataSets/getDataSetsChunkSecondSuccess")
    @Test
    public void shouldRetrieveDataSetsSecondChunk()
            throws Exception {
        String providerId = "Provider002";
        int resultSize = 100;
        String startFrom = "dataset000101";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        ResultSlice<DataSet> result = instance.getDataSetsForProviderChunk(providerId, startFrom);
        assertNotNull(result.getResults());
        assertThat(result.getResults().size(), is(resultSize));
        assertNull(result.getNextSlice());
    }

    @Betamax(tape = "dataSets/getDataSetsChunkNoProvider")
    @Test
    public void shouldNotThrowProviderNotExistsForDataSetsChunk()
            throws Exception {
        String providerId = "notFoundProviderId";
        String startFrom = null;

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        ResultSlice<DataSet> result = instance.getDataSetsForProviderChunk(providerId, startFrom);
        assertNotNull(result.getResults());
        assertThat(result.getResults().size(), is(0));
    }

    @Betamax(tape = "dataSets/getDataSetsSuccess")
    @Test
    public void shouldReturnAllDataSets()
            throws Exception {
        String providerId = "Provider002";
        int resultSize = 200;

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        List<DataSet> result = instance.getDataSetsForProvider(providerId);
        assertNotNull(result);
        assertThat(result.size(), is(resultSize));
    }

    @Betamax(tape = "dataSets/getDataSetsNoProvider")
    @Test
    public void shouldNotThrowProviderNotExistsForDataSetsAll()
            throws Exception {
        String providerId = "notFoundProviderId";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        List<DataSet> result = instance.getDataSetsForProvider(providerId);
        assertNotNull(result);
        assertThat(result.size(), is(0));
    }

    //to test it you can turn off Cassandra
    @Betamax(tape = "dataSets/getDataSetsChunkInternalServerError")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForGetDataSetsChunk()
            throws Exception {
        String providerId = "Provider001";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.getDataSetsForProviderChunk(providerId, null);
    }

    @Betamax(tape = "dataSets/getDataSetsInternalServerError")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForGetDataSets()
            throws Exception {
        String providerId = "Provider001";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.getDataSetsForProviderChunk(providerId, null);
    }

    @Betamax(tape = "dataSets/createDataSetSuccess")
    @Test
    public void shouldSuccessfullyCreateDataSet()
            throws Exception {
        String providerId = "Provider001";
        String dataSetId = "dataset000008";
        String description = "description01";

        String expectedLocation = baseUrl + "/data-providers/" + providerId + "/data-sets/" + dataSetId;

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        URI result = instance.createDataSet(providerId, dataSetId, description);
        assertThat(result.toString(), is(expectedLocation));
    }

    @Betamax(tape = "dataSets/createDataSetConflict")
    @Test(expected = DataSetAlreadyExistsException.class)
    public void shouldThrowDataSetAlreadyExists()
            throws Exception {
        String providerId = "Provider001";
        String dataSetId = "dataset000002";
        String description = "description";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.createDataSet(providerId, dataSetId, description);
    }

    @Betamax(tape = "dataSets/createDataSetProviderNotFound")
    @Test(expected = ProviderNotExistsException.class)
    public void shouldThrowProviderNotExists()
            throws Exception {
        String providerId = "notFoundProviderId";
        String dataSetId = "dataSetId";
        String description = "description";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.createDataSet(providerId, dataSetId, description);
    }

    @Betamax(tape = "dataSets/createDataSetInternalServerError")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForCreateDataSet()
            throws Exception {
        String providerId = "providerId";
        String dataSetId = "dataSetId";
        String description = "description";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.createDataSet(providerId, dataSetId, description);
    }

    @Betamax(tape = "dataSets/getRepresentationsChunkSuccess")
    @Test
    public void shouldRetrieveRepresentationsFirstChunk()
            throws Exception {
        String providerId = "Provider001";
        String dataSetId = "dataset000002";
        //the tape was recorded when the result chunk was 100
        int resultSize = 100;
        String startFrom = "G5DFUSCILJFVGQSEJYFHGY3IMVWWCMI=";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        ResultSlice<Representation> result = instance.getDataSetRepresentationsChunk(providerId, dataSetId, null);
        assertNotNull(result.getResults());
        assertThat(result.getResults().size(), is(resultSize));
        assertThat(result.getNextSlice(), is(startFrom));
    }

    @Betamax(tape = "dataSets/getRepresentationsChunkSecondSuccess")
    @Test
    public void shouldRetrieveRepresentationsSecondChunk()
            throws Exception {
        String providerId = "Provider001";
        String dataSetId = "dataset000002";
        int resultSize = 100;
        String startFrom = "G5DFUSCILJFVGQSEJYFHGY3IMVWWCMI=";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        ResultSlice<Representation> result = instance.getDataSetRepresentationsChunk(providerId, dataSetId, startFrom);
        assertNotNull(result.getResults());
        assertThat(result.getResults().size(), is(resultSize));
        assertNull(result.getNextSlice());
    }

    @Betamax(tape = "dataSets/getRepresentationsChunkDataSetNotExists")
    @Test(expected = DataSetNotExistsException.class)
    public void shouldThrowDataSetNotExistsForRepresentationsChunk()
            throws Exception {
        String providerId = "Provider001";
        String dataSetId = "dataset000042";
        String startFrom = "G5DFUSCILJFVGQSEJYFHGY3IMVWWCMI=";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.getDataSetRepresentationsChunk(providerId, dataSetId, startFrom);
    }

    @Betamax(tape = "dataSets/getRepresentationsSuccess")
    @Test
    public void shouldReturnAllRepresentations()
            throws Exception {
        String providerId = "Provider001";
        String dataSetId = "dataset000002";
        int resultSize = 200;

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        List<Representation> result = instance.getDataSetRepresentations(providerId, dataSetId);
        assertNotNull(result);
        assertThat(result.size(), is(resultSize));
    }

    @Betamax(tape = "dataSets/getRepresentationsDataSetNotExists")
    @Test(expected = DataSetNotExistsException.class)
    public void shouldThrowDataSetNotExistsForRepresentationsAll()
            throws Exception {
        String providerId = "Provider001";
        String dataSetId = "dataset000042";
        
        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.getDataSetRepresentations(providerId, dataSetId);
    }

    @Betamax(tape = "dataSets/getRepresentationsChunkInternalServerError")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForGetRepresentationsChunk()
            throws Exception {
        String providerId = "Provider001";
        String dataSetId = "dataset000002";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.getDataSetRepresentationsChunk(providerId, dataSetId, null);
    }

    @Betamax(tape = "dataSets/getRepresentationsInternalServerError")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForGetRepresentations()
            throws Exception {
        String providerId = "Provider001";
        String dataSetId = "dataset000002";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.getDataSetRepresentations(providerId, dataSetId);
    }

    //we cannot mock system state change in Betamax
    //because it will not record two different answers for the same request 
    @Betamax(tape = "dataSets/updateDescriptionSuccess")
    @Test
    public void ShouldUpdateDescriptionOfDataSet()
            throws Exception {
        String providerId = "Provider002";
        String dataSetId = "dataset000002";
        String description = "TEST1";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);

        instance.updateDescriptionOfDataSet(providerId, dataSetId, description);
        List<DataSet> dataSets = instance.getDataSetsForProvider(providerId);

        for (DataSet dataSet : dataSets) {
            if (dataSetId.equals(dataSet.getId())) {

                assertThat(dataSet.getDescription(), is(description));
            }

        }

    }

    @Betamax(tape = "dataSets/updateDescriptionEmptySuccess")
    @Test
    public void ShouldUpdateDescriptionOfDataSetToEmpty()
            throws Exception {
        String providerId = "Provider002";
        String dataSetId = "dataset000002";
        String description = "";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);

        instance.updateDescriptionOfDataSet(providerId, dataSetId, description);
        List<DataSet> dataSets = instance.getDataSetsForProvider(providerId);

        for (DataSet dataSet : dataSets) {
            if (dataSetId.equals(dataSet.getId())) {

                assertThat(dataSet.getDescription(), is(description));
            }

        }

    }

    @Betamax(tape = "dataSets/updateDescriptionNullSuccess")
    @Test
    public void ShouldUpdateDescriptionOfDataSetToNull()
            throws Exception {
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

    @Betamax(tape = "dataSets/updateDescriptionDataSetNotExists")
    @Test(expected = DataSetNotExistsException.class)
    public void shouldThrowDataSetNotExistsForUpdateDescription()
            throws Exception {
        String providerId = "Provider002";
        String dataSetId = "noSuchDataset";
        String description = "TEST4";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.updateDescriptionOfDataSet(providerId, dataSetId, description);
    }

    @Betamax(tape = "dataSets/updateDescriptionInternalServerError")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForUpdateDescription()
            throws Exception {
        String providerId = "Provider002";
        String dataSetId = "dataset000001";
        String description = "TEST3";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.updateDescriptionOfDataSet(providerId, dataSetId, description);
    }

    @Betamax(tape = "dataSets/deleteDataSetSuccess")
    @Test
    public void shouldDeleteDataSet()
            throws Exception {
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

    //TODO test this when fixed: https://jira.man.poznan.pl/jira/browse/ECL-141
    @Ignore
    //@Betamax(tape = "dataSets/deleteDataSetDataSetNotExists")
    @Test(expected = DataSetNotExistsException.class)
    public void shouldThrowDataSetNotExistsForDeleteDataSet()
            throws Exception {
        String providerId = "Provider002";
        String dataSetId = "dataset000033";
        DataSet dataSet = new DataSet();
        dataSet.setProviderId(providerId);
        dataSet.setId(dataSetId);
        dataSet.setDescription(null);

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.deleteDataSet(providerId, dataSetId);
    }

    @Betamax(tape = "dataSets/deleteDataSetInternalServerError")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverExceptionForDeleteDataSet()
            throws Exception {
        String providerId = "Provider002";
        String dataSetId = "dataset000033";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.deleteDataSet(providerId, dataSetId);

    }

}


//public void assignRepresentationToDataSet(String providerId, String dataSetId, String cloudId, String schemaId,
//public void unassignRepresentationToDataSet(String providerId, String dataSetId, String cloudId, String schemaId)

