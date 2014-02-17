package eu.europeana.cloud.mcs.driver;

import co.freeside.betamax.Betamax;
import co.freeside.betamax.Recorder;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import java.net.URI;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class DataSetServiceClientTest {

    @Rule
    public Recorder recorder = new Recorder();

    //TODO clean
    //this is only needed for recording tests
    private final String baseUrl = "http://localhost:8084/ecloud-service-mcs-rest-0.2-SNAPSHOT";
    private final String baseUrl2 = "http://localhost:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT";

    //getDataSetsForProviderChunk
    //getDataSetsForProvider
    
    @Betamax(tape = "dataSets/createDataSetSuccess")
    @Test
    public void shouldSuccessfullyCreateDataSet()
            throws Exception {
        String providerId = "providerId";
        String dataSetId = "dataSetId";
        String description = "description";

        String expectedLocation = baseUrl + "/data-providers/" + providerId + "/data-sets/" + dataSetId;

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        URI result = instance.createDataSet(providerId, dataSetId, description);
        assertThat(result.toString(), is(expectedLocation));
    }
    
    @Betamax(tape = "dataSets/createDataSetConflict")
    @Test(expected = DataSetAlreadyExistsException.class)
    public void shouldThrowDataSetAlreadyExists()
            throws Exception {
        String providerId = "providerId";
        String dataSetId = "dataSetId";
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

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl2);
        ResultSlice<Representation> result = instance.getDataSetRepresentationsChunk(providerId, dataSetId, null);
        assertNotNull(result.getResults());
        assertThat(result.getResults().size(), is(resultSize));
        assertThat(result.getNextSlice(), is(startFrom));
    }

    @Betamax(tape = "dataSets/getRepresentationsChunkSecondSucess")
    @Test
    public void shouldRetrieveRepresentationsSecondChunk()
            throws Exception {
        String providerId = "Provider001";
        String dataSetId = "dataset000002";
        int resultSize = 100;
        String startFrom = "G5DFUSCILJFVGQSEJYFHGY3IMVWWCMI=";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl2);
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

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl2);
        instance.getDataSetRepresentationsChunk(providerId, dataSetId, startFrom);
    }

    @Betamax(tape = "dataSets/getRepresentationsSuccess")
    @Test
    public void shouldReturnAllRepresentations()
            throws Exception {
        String providerId = "Provider001";
        String dataSetId = "dataset000002";
        int resultSize = 200;

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl2);
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

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl2);
        instance.getDataSetRepresentations(providerId, dataSetId);
    }

}


 //public void UpdateDescriptionOfDataSet(String providerId, String dataSetId, String description) throws DataSetNotExistsException, MCSException {
   // public void DeleteDataSet(String providerId, String dataSetId) throws DataSetNotExistsException, MCSException {
    //public void assignRepresentationToDataSet(String providerId, String dataSetId, String cloudId, String schemaId,
    //public void unassignRepresentationToDataSet(String providerId, String dataSetId, String cloudId, String schemaId)


