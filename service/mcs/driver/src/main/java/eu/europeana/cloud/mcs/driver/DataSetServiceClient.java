package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
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
 * Client for managing datasets in MCS.
 */
public class DataSetServiceClient {

    private final String baseUrl;
    private final Client client = ClientBuilder.newClient();
    private static final Logger logger = LoggerFactory.getLogger(DataSetServiceClient.class);

    //data-providers/{DATAPROVIDER}/data-sets
    private static final String dataSetsPath; // = ParamConstants.PROVIDERS + "/{" + ParamConstants.P_PROVIDER + "}/" + ParamConstants.DATASETS;
    //data-providers/{DATAPROVIDER}/data-sets/{DATASET}
    private static final String dataSetPath; // = dataSetsPath + "/{" + ParamConstants.P_DATASET + "}";
    //data-providers/{DATAPROVIDER}/data-sets/{DATASET}/assignments
    private static final String assignmentsPath; // = dataSetPath + "/" + ParamConstants.ASSIGNMENTS;

    static {
        StringBuilder builder = new StringBuilder();

        builder.append(ParamConstants.PROVIDERS);
        builder.append("/");
        builder.append("{");
        builder.append(ParamConstants.P_PROVIDER);
        builder.append("}/");
        builder.append(ParamConstants.DATASETS);
        dataSetsPath = builder.toString();

        builder.append("/");
        builder.append("{");
        builder.append(ParamConstants.P_DATASET);
        builder.append("}");
        dataSetPath = builder.toString();

        builder.append("/");
        builder.append(ParamConstants.ASSIGNMENTS);
        assignmentsPath = builder.toString();

    }

    /**
     * Creates instance of DataSetServiceClient.
     *
     * @param baseUrl MCS base address
     */
    public DataSetServiceClient(String baseUrl) {
        this.baseUrl = baseUrl;

    }

    /**
     * Returns chunk of data sets list of specified provider.
     *
     * This method returns the chunk specified by <code>startFrom</code>
     * parameter. If parameter is <code>null</code>, the first chunk is
     * returned. You can use {@link ResultSlice#getNextSlice()} of returned
     * result to obtain <code>startFrom</code> value to get the next chunk, etc;
     * if
     * {@link eu.europeana.cloud.common.response.ResultSlice#getNextSlice()}<code>==null</code>
     * in returned result it means it is the last slice.
     *
     * If you just need all representations, you can use
     * {@link #getDataSetRepresentations} method, which encapsulates this
     * method.
     *
     * @param providerId provider identifier (required)
     * @param startFrom code pointing to the requested result slice (if equal to
     * null, first slice is returned)
     * @return chunk of data sets list of specified provider (empty if provider
     * does not exist)
     * @throws MCSException on unexpected situations
     */
    public ResultSlice<DataSet> getDataSetsForProviderChunk(String providerId, String startFrom) throws MCSException {

        WebTarget target = client.target(this.baseUrl).path(dataSetsPath)
                .resolveTemplate(ParamConstants.P_PROVIDER, providerId);

        if (startFrom != null) {
            target = target.queryParam(ParamConstants.F_START_FROM, startFrom);
        }

        Response response = target.request().get();
        if (response.getStatus() == Status.OK.getStatusCode()) {
            return response.readEntity(ResultSlice.class);
        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }

    /**
     * Lists all data sets of specified provider.
     *
     * If provider does not exist, the empty list is returned.
     *
     * @param providerId provider identifier (required)
     * @return list of all data sets of specified provider (empty if provider
     * does not exist)
     * @throws MCSException on unexpected situations
     */
    public List<DataSet> getDataSetsForProvider(String providerId) throws MCSException {

        List<DataSet> resultList = new ArrayList<>();
        ResultSlice resultSlice;
        String startFrom = null;

        do {
            resultSlice = getDataSetsForProviderChunk(providerId, startFrom);
            if (resultSlice == null || resultSlice.getResults() == null) {
                throw new DriverException("Getting DataSet: result chunk obtained but is empty.");
            }
            resultList.addAll(resultSlice.getResults());
            startFrom = resultSlice.getNextSlice();

        } while (resultSlice.getNextSlice() != null);

        return resultList;
    }

    /**
     * Creates a new data set.
     *
     * @param providerId provider identifier (required)
     * @param dataSetId data set identifier (required)
     * @param description data set description (not required)
     * @return URI to created data set
     * @throws DataSetAlreadyExistsException when data set with given id (for
     * given provider) already exists
     * @throws ProviderNotExistsException when provider with given id does not
     * exist
     * @throws MCSException on unexpected situations
     */
    public URI createDataSet(String providerId, String dataSetId, String description)
            throws ProviderNotExistsException, DataSetAlreadyExistsException, MCSException {

        WebTarget target = client.target(this.baseUrl).path(dataSetsPath)
                .resolveTemplate(ParamConstants.P_PROVIDER, providerId);

        Form form = new Form();
        form.param(ParamConstants.F_DATASET, dataSetId);
        form.param(ParamConstants.F_DESCRIPTION, description);

        Response response = target.request().post(
                Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));

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

    /**
     * Returns chunk of representation versions list from data set.
     *
     * If specific version of representation is assigned to data set, this
     * version is returned. If a whole representation is assigned to data set,
     * the latest persistent representation version is returned.
     *
     * This method returns the chunk specified by <code>startFrom</code>
     * parameter. If parameter is empty, the first chunk is returned. You can
     * use {@link ResultSlice#getNextSlice()} of returned result to obtain
     * <code>startFrom</code> value to get the next chunk, etc. If you just need
     * all representations, you can use {@link #getDataSetRepresentations}
     * method, which encapsulates this method.
     *
     * @param providerId provider identifier (required)
     * @param dataSetId data set identifier (required)
     * @param startFrom code pointing to the requested result slice (if equal to
     * null, first slice is returned)
     * @return chunk of representation versions list from data set
     * @throws DataSetNotExistsException if data set does not exist
     * @throws MCSException on unexpected situations
     */
    public ResultSlice<Representation> getDataSetRepresentationsChunk(String providerId, String dataSetId, String startFrom) throws DataSetNotExistsException, MCSException {
        WebTarget target = client.target(this.baseUrl).path(dataSetPath)
                .resolveTemplate(ParamConstants.P_PROVIDER, providerId)
                .resolveTemplate(ParamConstants.P_DATASET, dataSetId);

        if (startFrom != null) {
            target = target.queryParam(ParamConstants.F_START_FROM, startFrom);
        }

        Response response = target.request().get();
        if (response.getStatus() == Status.OK.getStatusCode()) {
            return response.readEntity(ResultSlice.class);
        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }

    /**
     * Lists all representation versions from data set.
     *
     * If specific version of representation is assigned to data set, this
     * version is returned. If a whole representation is assigned to data set,
     * the latest persistent representation version is returned.
     *
     * @param providerId provider identifier (required)
     * @param dataSetId data set identifier (required)
     * @return list of representation versions from data set
     * @throws DataSetNotExistsException if data set does not exist
     * @throws MCSException on unexpected situations
     */
    public List<Representation> getDataSetRepresentations(String providerId, String dataSetId) throws DataSetNotExistsException, MCSException {

        List<Representation> resultList = new ArrayList<>();
        ResultSlice resultSlice;
        String startFrom = null;

        do {
            resultSlice = getDataSetRepresentationsChunk(providerId, dataSetId, startFrom);
            if (resultSlice == null || resultSlice.getResults() == null) {
                throw new DriverException("Getting DataSet: result chunk obtained but is empty.");
            }
            resultList.addAll(resultSlice.getResults());
            startFrom = resultSlice.getNextSlice();

        } while (resultSlice.getNextSlice() != null);

        return resultList;
    }

    /**
     * Updates description of data set.
     *
     * @param providerId provider identifier (required)
     * @param dataSetId data set identifier (required)
     * @param description new description of data set (if <code>""</code> will
     * be set to <code>""</code>, if <code>null</code> will be set to
     * <code>null</code>)
     * @throws DataSetNotExistsException if data set does not exist
     * @throws MCSException on unexpected situations
     */
    public void updateDescriptionOfDataSet(String providerId, String dataSetId, String description) throws DataSetNotExistsException, MCSException {
        WebTarget target = client.target(this.baseUrl).path(dataSetPath)
                .resolveTemplate(ParamConstants.P_PROVIDER, providerId)
                .resolveTemplate(ParamConstants.P_DATASET, dataSetId);

        Form form = new Form();
        form.param(ParamConstants.F_DESCRIPTION, description);

        Response response = target.request().put(
                Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));

        if (response.getStatus() != Status.NO_CONTENT.getStatusCode()) {

            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }

    /**
     * Deletes data set.
     *
     * @param providerId provider identifier (required)
     * @param dataSetId data set identifier (required)
     * @throws DataSetNotExistsException if data set does not exist
     * @throws MCSException on unexpected situations
     */
    public void deleteDataSet(String providerId, String dataSetId) throws DataSetNotExistsException, MCSException {
        WebTarget target = client.target(this.baseUrl).path(dataSetPath)
                .resolveTemplate(ParamConstants.P_PROVIDER, providerId)
                .resolveTemplate(ParamConstants.P_DATASET, dataSetId);

        Response response = target.request().delete();

        if (response.getStatus() != Status.NO_CONTENT.getStatusCode()) {

            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }

    }

    /**
     * Assigns representation into data set.
     *
     * If specific version is assigned, and then other version of the same
     * schema assigned again, the old version is overridden. You can also assign
     * the representation without version in this case the old version will also
     * be overridden. Note that the version number will be then set to null in
     * Cassandra, but
     * {@link #getDataSetRepresentations(java.lang.String, java.lang.String)}
     * method will return the last persistent version with
     * {@link Representation#version} filled.
     *
     * @param providerId provider identifier (required)
     * @param dataSetId data set identifier (required)
     * @param cloudId cloudId of the record (required)
     * @param schemaId schema of the representation (required)
     * @param versionId version of representation; if not provided, latest
     * persistent version will be assigned to data set
     * @throws DataSetNotExistsException if data set does not exist
     * @throws RepresentationNotExistsException if no such representation exists
     * @throws MCSException on unexpected situations
     */
    public void assignRepresentationToDataSet(String providerId, String dataSetId, String cloudId, String schemaId,
            String versionId) throws DataSetNotExistsException, RepresentationNotExistsException, MCSException {

        WebTarget target = client.target(this.baseUrl).path(assignmentsPath)
                .resolveTemplate(ParamConstants.P_PROVIDER, providerId)
                .resolveTemplate(ParamConstants.P_DATASET, dataSetId);

        Form form = new Form();
        form.param(ParamConstants.F_GID, cloudId);
        form.param(ParamConstants.F_SCHEMA, schemaId);
        form.param(ParamConstants.F_VER, versionId);

        Response response = target.request().post(
                Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));

        if (response.getStatus() != Status.NO_CONTENT.getStatusCode()) {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }

    }

    /**
     * Unassigns representation from data set.
     *
     * If representation was not assigned to data set, nothing happens. If
     * representation does not exist, nothing happens.
     *
     * @param providerId provider identifier (required)
     * @param dataSetId data set identifier (required)
     * @param cloudId cloudId of the record (required)
     * @param schemaId schema of the representation (required)
     * @throws DataSetNotExistsException if data set does not exist
     * @throws MCSException on unexpected situations
     */
    public void unassignRepresentationFromDataSet(String providerId, String dataSetId, String cloudId, String schemaId)
            throws DataSetNotExistsException, MCSException {

        WebTarget target = client.target(this.baseUrl).path(assignmentsPath)
                .resolveTemplate(ParamConstants.P_PROVIDER, providerId)
                .resolveTemplate(ParamConstants.P_DATASET, dataSetId)
                .queryParam(ParamConstants.F_GID, cloudId)
                .queryParam(ParamConstants.F_SCHEMA, schemaId);

        Response response = target.request().delete();

        if (response.getStatus() != Status.NO_CONTENT.getStatusCode()) {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }

    }

}
