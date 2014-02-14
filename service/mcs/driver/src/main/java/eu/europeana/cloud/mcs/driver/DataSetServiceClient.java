package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
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

    /**
     * Creates instance of DataSetServiceClient.
     *
     * @param baseUrl MCS base address
     */
    public DataSetServiceClient(String baseUrl) {
        this.baseUrl = baseUrl;

    }

    /**
     * Creates a new data set in MCS.
     *
     * @param providerId provider identifier
     * @param dataSetId data set identifier
     * @param description data set description.
     * @return URI to created data set
     * @throws DataSetAlreadyExistsException when data set with given id (for
     * given provider) already exists
     * @throws ProviderNotExistsException when provider with given id does not
     * exits
     * @throws MCSException on unexpected situations.
     */
    public URI createDataSet(String providerId, String dataSetId, String description)
            throws ProviderNotExistsException, DataSetAlreadyExistsException, MCSException {

        WebTarget target = client.target(this.baseUrl).path("data-providers/{DATAPROVIDER}/data-sets/")
                .resolveTemplate("DATAPROVIDER", providerId);

        Form form = new Form();
        form.param("dataSetId", dataSetId);
        form.param("description", description);

        Response response = target.request().post(
                Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));

        Response.StatusType statusInfo = response.getStatusInfo();
        if (response.getStatus() == Status.CREATED.getStatusCode()) {
            return response.getLocation();
        } else {
            //TODO this does not function correctly,
            //details are filled with "MessageBodyReader not found for media type=text/html; 
            //charset=utf-8, type=class eu.europeana.cloud.common.response.ErrorInfo, 
            //genericType=class eu.europeana.cloud.common.response.ErrorInfo."
            //simple strings like 'adsfd' get entitised correctly
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);

        }

    }

    public List<Representation> getDataSet(String providerId, String dataSetId) throws DataSetNotExistsException, MCSException {

        List<Representation> resultList = new ArrayList<>();
        ResultSlice resultSlice;
        String startFrom = null;
        
        do {
            resultSlice = getDataSetChunk(providerId, dataSetId, startFrom);
            if (resultSlice == null || resultSlice.getResults() == null) {
                throw new DriverException("Getting DataSet: result chunk obtained but is empty.");
            }
            resultList.addAll(resultSlice.getResults());
            startFrom = resultSlice.getNextSlice();

        } while (resultSlice.getNextSlice() != null);

        return resultList;
    }

    public ResultSlice<Representation> getDataSetChunk(String providerId, String dataSetId, String startFrom) throws DataSetNotExistsException, MCSException {
        WebTarget target = client.target(this.baseUrl).path("data-providers/{DATAPROVIDER}/data-sets/{DATASETID}")
                .resolveTemplate("DATAPROVIDER", providerId)
                .resolveTemplate("DATASETID", dataSetId);
        
        if(startFrom!=null)
        {
            target = target.queryParam("startFrom", startFrom);
        }
        
        Response response = target.request().get();
        if (response.getStatus() == Status.OK.getStatusCode()) {
            return response.readEntity(ResultSlice.class);
        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }

    public void getDataSetForProvider(String providerId) {
    }

    public void assignRepresentationToDataSet(String dataSetId, String providerId, String cloudId, String schemaId,
            String versionId) {
    }

}
