package eu.europeana.cloud.mcs.driver;

import co.freeside.betamax.Betamax;
import co.freeside.betamax.Recorder;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import java.net.URI;
import org.junit.Rule;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Ignore;

public class DataSetServiceClientTest {

    @Rule
    public Recorder recorder = new Recorder();

    private final String baseUrl = "http://localhost:8084/ecloud-service-mcs-rest-0.2-SNAPSHOT";


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
    public void shouldThrowProviderNotExistEx()
            throws Exception {
        String providerId = "notFoundProviderId";
        String dataSetId = "dataSetId";
        String description = "description";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.createDataSet(providerId, dataSetId, description);
    }


    @Ignore
    @Betamax(tape = "dataSets/createDataSetInternalServerError")
    @Test(expected = DriverException.class)
    public void shouldThrowDriverException()
            throws Exception {
        String providerId = "notFoundProviderId";
        String dataSetId = "dataSetId";
        String description = "description";

        DataSetServiceClient instance = new DataSetServiceClient(baseUrl);
        instance.createDataSet(providerId, dataSetId, description);
    }

}
