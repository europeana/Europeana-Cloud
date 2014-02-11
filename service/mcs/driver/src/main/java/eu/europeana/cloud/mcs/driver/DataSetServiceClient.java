package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.mcs.driver.exception.ServiceInternalErrorException;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import java.net.URI;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.ws.rs.core.Response.Status;

/**
 * Client for managing datasets.
 */
public class DataSetServiceClient {

    private final String baseUrl;
    private final Client client = ClientBuilder.newClient();
    private static final Logger logger = LoggerFactory.getLogger(DataSetServiceClient.class);


    public DataSetServiceClient(String baseUrl) {
        this.baseUrl = baseUrl;

    }


    /**
     * Creates a new data set in MCS.
     * 
     * @param providerId
     *            provider identifier
     * @param dataSetId
     *            data set identifier
     * @param description
     *            data set description.
     * @return URI to created data set
     * @throws DataSetAlreadyExistsException
     *             when data set with given id (for given provider) already exists
     * @throws ProviderNotExistsException
     *             when provider with given id does not exits
     */
    public URI createDataSet(String providerId, String dataSetId, String description)
            throws DataSetAlreadyExistsException, ProviderNotExistsException {

        WebTarget target = client.target(this.baseUrl).path("data-providers/{DATAPROVIDER}/data-sets/")
                .resolveTemplate("DATAPROVIDER", providerId);

        Form form = new Form();
        form.param("dataSetId", dataSetId);
        form.param("description", description);

        Response response = target.request().post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));

        int statusCode = response.getStatus();
        Response.StatusType statusInfo = response.getStatusInfo();
        if (statusCode == Status.CREATED.getStatusCode()) {
            return response.getLocation();
        } else if (statusCode == Status.CONFLICT.getStatusCode()) {
            throw new DataSetAlreadyExistsException(statusInfo.getReasonPhrase());
        } else if (statusCode == Status.NOT_FOUND.getStatusCode()) {
            throw new ProviderNotExistsException(statusInfo.getReasonPhrase());
        } else {
            throw new ServiceInternalErrorException(statusInfo.getReasonPhrase());
        }
    }


    public DataSet getDataSet(String providerId, String dataSetId) {
        return null;
    }


    public void getDataSetForProvider(String providerId) {
    }


    public void assignRepresentationToDataSet(String dataSetId, String providerId, String cloudId, String schemaId,
            String versionId) {
    }

}
