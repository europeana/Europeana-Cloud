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
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

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

    //
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

    //to test it you can turn off Cassandra
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

}

//public void updateDescriptionOfDataSet(String providerId, String dataSetId, String description) throws DataSetNotExistsException, MCSException {
// public void deleteDataSet(String providerId, String dataSetId) throws DataSetNotExistsException, MCSException {
//public void assignRepresentationToDataSet(String providerId, String dataSetId, String cloudId, String schemaId,
//public void unassignRepresentationToDataSet(String providerId, String dataSetId, String cloudId, String schemaId)

