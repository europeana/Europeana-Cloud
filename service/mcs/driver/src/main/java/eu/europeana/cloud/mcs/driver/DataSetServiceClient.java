package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.filter.ECloudBasicAuthFilter;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.*;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static eu.europeana.cloud.common.web.ParamConstants.*;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.*;

/**
 * Client for managing datasets in MCS.
 */
public class DataSetServiceClient extends MCSClient {

    /**
     * Creates instance of DataSetServiceClient.
     *
     * @param baseUrl URL of the MCS Rest Service
     */
    public DataSetServiceClient(String baseUrl) {
        this(baseUrl, null, null);
    }

    public DataSetServiceClient(String baseUrl, final String authorization) {
        this(baseUrl, authorization, null, null, DEFAULT_CONNECT_TIMEOUT_IN_MILLIS, DEFAULT_READ_TIMEOUT_IN_MILLIS);
    }

    /**
     * Creates instance of DataSetServiceClient. Same as {@link #DataSetServiceClient(String)}
     * but includes username and password to perform authenticated requests.
     *
     * @param baseUrl URL of the MCS Rest Service
     */
    public DataSetServiceClient(String baseUrl, final String username, final String password) {
        this(baseUrl,null, username, password, DEFAULT_CONNECT_TIMEOUT_IN_MILLIS, DEFAULT_READ_TIMEOUT_IN_MILLIS);
    }

    /**
     * All parameters constructor used by another one
     * @param baseUrl URL of the MCS Rest Service
     * @param authorizationHeader Authorization header - used instead username/password pair
     * @param username Username to HTTP authorisation  (use together with password)
     * @param password Password to HTTP authorisation (use together with username)
     * @param connectTimeoutInMillis Timeout for waiting for connecting
     * @param readTimeoutInMillis Timeout for getting data
     */
    public DataSetServiceClient(String baseUrl, final String authorizationHeader, final String username, final String password,
                                final int connectTimeoutInMillis, final int readTimeoutInMillis) {
        super(baseUrl);

        if(authorizationHeader != null) {
            this.client.register(new ECloudBasicAuthFilter(authorizationHeader));
        } else if(username != null || password != null) {
            this.client.register(HttpAuthenticationFeature.basicBuilder().credentials(username, password).build());
        }
        this.client.property(ClientProperties.CONNECT_TIMEOUT, connectTimeoutInMillis);
        this.client.property(ClientProperties.READ_TIMEOUT, readTimeoutInMillis);
    }

    /**
     * Returns chunk of data sets list of specified provider.
     * <p/>
     * This method returns the chunk specified by <code>startFrom</code>
     * parameter. If parameter is <code>null</code>, the first chunk is
     * returned. You can use {@link ResultSlice#getNextSlice()} of returned
     * result to obtain <code>startFrom</code> value to get the next chunk, etc.;
     * if
     * {@link eu.europeana.cloud.common.response.ResultSlice#getNextSlice()}<code>==null</code>
     * in returned result it means it is the last slice.
     * <p/>
     * If you just need all representations, you can use
     * {@link #getDataSetRepresentations} method, which encapsulates this
     * method.
     *
     * @param providerId provider identifier (required)
     * @param startFrom  code pointing to the requested result slice (if equal to
     *                   null, first slice is returned)
     * @return chunk of data sets list of specified provider (empty if provider
     * does not exist)
     * @throws MCSException on unexpected situations
     */
    @SuppressWarnings("unchecked")
    public ResultSlice<DataSet> getDataSetsForProviderChunk(String providerId, String startFrom) throws MCSException {
        if(startFrom == null) {
            return getDataSetsForProviderChunk(providerId);
        } else {
            return manageResponse(ResultSlice.class, Status.OK,
                    () -> client
                            .target(this.baseUrl)
                            .path(DATA_SETS_RESOURCE)
                            .resolveTemplate(PROVIDER_ID, providerId)
                            .queryParam(ParamConstants.F_START_FROM, startFrom)
                            .request()
                            .get()
            );
        }
    }

    @SuppressWarnings("unchecked")
    private ResultSlice<DataSet> getDataSetsForProviderChunk(String providerId) throws MCSException {
        return manageResponse(ResultSlice.class, Status.OK,
                () -> client
                        .target(this.baseUrl)
                        .path(DATA_SETS_RESOURCE)
                        .resolveTemplate(PROVIDER_ID, providerId)
                        .request()
                        .get()
        );
    }

    @Deprecated
    public ResultSlice<DataSet> getDataSetsForProviderChunkOld(String providerId, String startFrom) throws MCSException {

        WebTarget target = client
                .target(this.baseUrl)
                .path(DATA_SETS_RESOURCE)
                .resolveTemplate(PROVIDER_ID, providerId);

        if (startFrom != null) {
            target = target.queryParam(ParamConstants.F_START_FROM, startFrom);
        }

        return prepareResultSliceResponse(target);
    }

    @Deprecated
    private ResultSlice prepareResultSliceResponse(WebTarget target) throws MCSException {
        Response response = null;
        try {
            response = target.request().get();
            if (response.getStatus() == Status.OK.getStatusCode()) {
                return response.readEntity(ResultSlice.class);
            } else {
                ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
                throw MCSExceptionProvider.generateException(errorInfo);
            }
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }



    /**
     * Lists all data sets of specified provider.
     * <p/>
     * If provider does not exist, the empty list is returned.
     *
     * @param providerId provider identifier (required)
     * @return list of all data sets of specified provider (empty if provider
     * does not exist)
     * @throws MCSException on unexpected situations
     */
    public List<DataSet> getDataSetsForProvider(String providerId) throws MCSException {

        List<DataSet> resultList = new ArrayList<>();
        ResultSlice<DataSet> resultSlice;
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
     * Returns iterator to the list of all data sets of specified provider.
     * <p/>
     * If provider does not exist, the iterator returned will be empty. Iterator
     * is not initialised with data on creation, calls to MCS server are
     * performed in iterator methods.
     *
     * @param providerId provider identifier (required)
     * @return iterator to the list of all data sets of specified provider
     * (empty if provider does not exist)
     */
    public DataSetIterator getDataSetIteratorForProvider(String providerId) {
        return new DataSetIterator(this, providerId);
    }

    /**
     * Creates a new data set.
     *
     * @param providerId  provider identifier (required)
     * @param dataSetId   data set identifier (required)
     * @param description data set description (not required)
     * @return URI to created data set
     * @throws DataSetAlreadyExistsException when data set with given id (for
     *                                       given provider) already exists
     * @throws ProviderNotExistsException    when provider with given id does not
     *                                       exist
     * @throws MCSException                  on unexpected situations
     */
    public URI createDataSet(String providerId, String dataSetId, String description) throws MCSException {
        Form form = new Form();
        form.param(ParamConstants.F_DATASET, dataSetId);
        form.param(ParamConstants.F_DESCRIPTION, description);

        return manageResponse(URI.class, Status.CREATED,
                () -> client
                        .target(this.baseUrl)
                        .path(DATA_SETS_RESOURCE)
                        .resolveTemplate(PROVIDER_ID, providerId)
                        .request()
                        .post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE))
        );
    }

    @Deprecated
    public URI createDataSetOld(String providerId, String dataSetId, String description) throws MCSException {

        WebTarget target = client
                .target(this.baseUrl)
                .path(DATA_SETS_RESOURCE)
                .resolveTemplate(PROVIDER_ID, providerId);

        Form form = new Form();
        form.param(ParamConstants.F_DATASET, dataSetId);
        form.param(ParamConstants.F_DESCRIPTION, description);

        Response response = null;

        try {
            response = target
                    .request()
                    .post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));

            if (response.getStatus() == Status.CREATED.getStatusCode()) {
                return response.getLocation();
            }

            //TODO this does not function correctly,
            //details are filled with "MessageBodyReader not found for media type=text/html; 
            //charset=utf-8, type=class eu.europeana.cloud.common.response.ErrorInfo, 
            //genericType=class eu.europeana.cloud.common.response.ErrorInfo."
            //simple strings like 'abc' get entitsed correctly
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    /**
     * Returns chunk of representation versions list from data set.
     * <p/>
     * If specific version of representation is assigned to data set, this
     * version is returned. If a whole representation is assigned to data set,
     * the latest persistent representation version is returned.
     * <p/>
     * This method returns the chunk specified by <code>startFrom</code>
     * parameter. If parameter is empty, the first chunk is returned. You can
     * use {@link ResultSlice#getNextSlice()} of returned result to obtain
     * <code>startFrom</code> value to get the next chunk, etc. If you just need
     * all representations, you can use {@link #getDataSetRepresentations}
     * method, which encapsulates this method.
     *
     * @param providerId provider identifier (required)
     * @param dataSetId  data set identifier (required)
     * @param startFrom  code pointing to the requested result slice (if equal to
     *                   null, first slice is returned)
     * @return chunk of representation versions list from data set
     * @throws DataSetNotExistsException if data set does not exist
     * @throws MCSException              on unexpected situations
     */
    @SuppressWarnings("unchecked")
    public ResultSlice<Representation> getDataSetRepresentationsChunk(String providerId, String dataSetId, String startFrom) throws MCSException {
        if(startFrom == null) {
            return getDataSetRepresentationsChunk(providerId, dataSetId);
        } else {
            return manageResponse(ResultSlice.class, Status.OK,
                    () -> client
                            .target(this.baseUrl)
                            .path(DATA_SET_RESOURCE)
                            .resolveTemplate(PROVIDER_ID, providerId)
                            .resolveTemplate(DATA_SET_ID, dataSetId)
                            .queryParam(ParamConstants.F_START_FROM, startFrom)
                            .request()
                            .get()
            );
        }
    }

    private ResultSlice<Representation> getDataSetRepresentationsChunk(String providerId, String dataSetId) throws MCSException {
        return manageResponse(ResultSlice.class, Status.OK,
                () -> client
                        .target(this.baseUrl)
                        .path(DATA_SET_RESOURCE)
                        .resolveTemplate(PROVIDER_ID, providerId)
                        .resolveTemplate(DATA_SET_ID, dataSetId)
                        .request()
                        .get()
        );
    }

    @Deprecated
    public ResultSlice<Representation> getDataSetRepresentationsChunkOld(String providerId, String dataSetId, String startFrom) throws MCSException {
        
        WebTarget target = client
                .target(this.baseUrl)
                .path(DATA_SET_RESOURCE)
                .resolveTemplate(PROVIDER_ID, providerId)
                .resolveTemplate(DATA_SET_ID, dataSetId);

        if (startFrom != null) {
            target = target.queryParam(ParamConstants.F_START_FROM, startFrom);
        }

        Response response = null;
        try {
            response = target.request().get();
            if (response.getStatus() == Status.OK.getStatusCode()) {
                return response.readEntity(ResultSlice.class);
            }
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    /**
     * Lists all representation versions from data set.
     * <p/>
     * If specific version of representation is assigned to data set, this
     * version is returned. If a whole representation is assigned to data set,
     * the latest persistent representation version is returned.
     *
     * @param providerId provider identifier (required)
     * @param dataSetId  data set identifier (required)
     * @return list of representation versions from data set
     * @throws DataSetNotExistsException if data set does not exist
     * @throws MCSException              on unexpected situations
     */
    public List<Representation> getDataSetRepresentations(String providerId, String dataSetId) throws MCSException {
        List<Representation> resultList = new ArrayList<>();
        ResultSlice<Representation> resultSlice;
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
     * Returns iterator to list of representation versions of data set.
     * <p/>
     * Iterator is not initialised with data on creation, calls to MCS server
     * are performed in iterator methods.
     *
     * @param providerId provider identifier (required)
     * @param dataSetId  data set identifier (required)
     * @return iterator to the list of all data sets of specified provider
     * (empty if provider does not exist)
     */
    public RepresentationIterator getRepresentationIterator(String providerId, String dataSetId) {
        return new RepresentationIterator(this, providerId, dataSetId);
    }

    /**
     * Updates description of data set.
     *
     * @param providerId  provider identifier (required)
     * @param dataSetId   data set identifier (required)
     * @param description new description of data set (if <code>""</code> will
     *                    be set to <code>""</code>, if <code>null</code> will be set to
     *                    <code>null</code>)
     * @throws DataSetNotExistsException if data set does not exist
     * @throws MCSException              on unexpected situations
     */
    public void updateDescriptionOfDataSet(String providerId, String dataSetId, String description) throws MCSException {
        Form form = new Form();
        form.param(ParamConstants.F_DESCRIPTION, description);

        manageResponse(Void.class, Status.NO_CONTENT,
                () -> client
                        .target(this.baseUrl)
                        .path(DATA_SET_RESOURCE)
                        .resolveTemplate(PROVIDER_ID, providerId)
                        .resolveTemplate(DATA_SET_ID, dataSetId)
                        .request()
                        .put(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE))
        );
    }

    @Deprecated
    public void updateDescriptionOfDataSetOld(String providerId, String dataSetId, String description) throws MCSException {
        
        WebTarget target = client
                .target(this.baseUrl)
                .path(DATA_SET_RESOURCE)
                .resolveTemplate(PROVIDER_ID, providerId)
                .resolveTemplate(DATA_SET_ID, dataSetId);

        Form form = new Form();
        form.param(ParamConstants.F_DESCRIPTION, description);

        Response response = null;

        try {
            response = target.request().put(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));

            if (response.getStatus() != Status.NO_CONTENT.getStatusCode()) {

                ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
                throw MCSExceptionProvider.generateException(errorInfo);
            }
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    /**
     * Deletes data set.
     *
     * @param providerId provider identifier (required)
     * @param dataSetId  data set identifier (required)
     * @throws DataSetNotExistsException if data set does not exist
     * @throws MCSException              on unexpected situations
     */
    public void deleteDataSet(String providerId, String dataSetId) throws MCSException {
        manageResponse(Void.class, Status.NO_CONTENT,
                () -> client
                        .target(this.baseUrl)
                        .path(DATA_SET_RESOURCE)
                        .resolveTemplate(PROVIDER_ID, providerId)
                        .resolveTemplate(DATA_SET_ID, dataSetId)
                        .request()
                        .delete()
        );
    }

    @Deprecated
    public void deleteDataSetOld(String providerId, String dataSetId) throws MCSException {

        WebTarget target = client
                .target(this.baseUrl)
                .path(DATA_SET_RESOURCE)
                .resolveTemplate(PROVIDER_ID, providerId)
                .resolveTemplate(DATA_SET_ID, dataSetId);

        Response response = null;

        try {
            response = target.request().delete();

            if (response.getStatus() != Status.NO_CONTENT.getStatusCode()) {

                ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
                throw MCSExceptionProvider.generateException(errorInfo);
            }
        } finally {
            if (response != null) {
                response.close();
            }
        }


    }

    /**
     * Assigns representation into data set.
     * <p/>
     * If specific version is assigned, and then other version of the same
     * representation name assigned again, the old version is overridden. You
     * can also assign the representation without version in this case the old
     * version will also be overridden. Note that the version number will be
     * then set to null in Cassandra, but
     * {@link #getDataSetRepresentations(java.lang.String, java.lang.String)}
     * method will return the last persistent version with
     * {@link Representation#setVersion(String)} filled.
     *
     * @param providerId         provider identifier (required)
     * @param dataSetId          data set identifier (required)
     * @param cloudId            cloudId of the record (required)
     * @param representationName name of the representation (required)
     * @param version            version of representation; if not provided, the latest one latest
     *                           persistent version will be assigned to data set
     * @throws DataSetNotExistsException        if data set does not exist
     * @throws RepresentationNotExistsException if no such representation exists
     * @throws MCSException                     on unexpected situations
     */
    public void assignRepresentationToDataSet(String providerId, String dataSetId,
                                              String cloudId, String representationName, String version) throws MCSException {

        Form form = getForm(cloudId, representationName, version);

        manageResponse(Void.class, Status.NO_CONTENT,
                () -> client
                        .target(this.baseUrl)
                        .path(DATA_SET_ASSIGNMENTS)
                        .resolveTemplate(PROVIDER_ID, providerId)
                        .resolveTemplate(DATA_SET_ID, dataSetId)
                        .request()
                        .post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE))
        );
    }

    @Deprecated
    public void assignRepresentationToDataSetOld(String providerId, String dataSetId,
                                              String cloudId, String representationName, String version) throws MCSException {
        
        WebTarget target = client
                .target(this.baseUrl)
                .path(DATA_SET_ASSIGNMENTS)
                .resolveTemplate(PROVIDER_ID, providerId)
                .resolveTemplate(DATA_SET_ID, dataSetId);

        Form form = getForm(cloudId, representationName, version);


        Response response = null;

        try {
            response = target.request().post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));

            if (response.getStatus() != Status.NO_CONTENT.getStatusCode()) {
                ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
                throw MCSExceptionProvider.generateException(errorInfo);
            }

        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    /**
     * Assigns representation into data set.
     * <p/>
     * If specific version is assigned, and then other version of the same
     * representation name assigned again, the old version is overridden. You
     * can also assign the representation without version in this case the old
     * version will also be overridden. Note that the version number will be
     * then set to null in Cassandra, but
     * {@link #getDataSetRepresentations(java.lang.String, java.lang.String)}
     * method will return the last persistent version with
     * {@link Representation#setVersion(String)} filled.
     *
     * @param providerId         provider identifier (required)
     * @param dataSetId          data set identifier (required)
     * @param cloudId            cloudId of the record (required)
     * @param representationName name of the representation (required)
     * @param version            version of representation; if not provided, the latest one
     *                           persistent version will be assigned to data set
     * @param key                key of header request
     * @param value              value of header request
     * @throws DataSetNotExistsException        if data set does not exist
     * @throws RepresentationNotExistsException if no such representation exists
     * @throws MCSException                     on unexpected situations
     */
    public void assignRepresentationToDataSet(String providerId, String dataSetId, String cloudId,
                                              String representationName, String version, String key, String value) throws MCSException {

        Form form = getForm(cloudId, representationName, version);
        manageResponse(Void.class, Status.NO_CONTENT,
                () -> client
                        .target(this.baseUrl)
                        .path(DATA_SET_ASSIGNMENTS)
                        .resolveTemplate(PROVIDER_ID, providerId)
                        .resolveTemplate(DATA_SET_ID, dataSetId)
                        .request()
                        .header(key, value)
                        .post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE))
        );
    }

    @Deprecated
    public void assignRepresentationToDataSetOld(String providerId, String dataSetId, String cloudId,
                                              String representationName, String version, String key, String value) throws MCSException {
        
        WebTarget target = client
                .target(this.baseUrl)
                .path(DATA_SET_ASSIGNMENTS)
                .resolveTemplate(PROVIDER_ID, providerId)
                .resolveTemplate(DATA_SET_ID, dataSetId);

        Form form = getForm(cloudId, representationName, version);
        
        Response response = null;

        try {
            response = target
                    .request()
                    .header(key, value)
                    .post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));

            if (response.getStatus() != Status.NO_CONTENT.getStatusCode()) {
                ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
                throw MCSExceptionProvider.generateException(errorInfo);
            }
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }


    /**
     * Un-assigns representation from data set.
     * <p/>
     * If representation was not assigned to data set, nothing happens. If
     * representation does not exist, nothing happens.
     *
     * @param providerId         provider identifier (required)
     * @param dataSetId          data set identifier (required)
     * @param cloudId            cloudId of the record (required)
     * @param representationName name of the representation (required)
     * @param version            version of the representation
     * @throws DataSetNotExistsException if data set does not exist
     * @throws MCSException              on unexpected situations
     */
    public void unassignRepresentationFromDataSet(
            String providerId, String dataSetId, String cloudId, String representationName, String version) throws MCSException {
        manageResponse(Void.class, Status.NO_CONTENT,
                () -> client
                        .target(this.baseUrl)
                        .path(DATA_SET_ASSIGNMENTS)
                        .resolveTemplate(PROVIDER_ID, providerId)
                        .resolveTemplate(DATA_SET_ID, dataSetId)
                        .queryParam(CLOUD_ID, cloudId)
                        .queryParam(REPRESENTATION_NAME, representationName)
                        .queryParam(VERSION, version)
                        .request()
                        .delete()
        );
    }

    @Deprecated
    public void unassignRepresentationFromDataSetOld(
            String providerId, String dataSetId, String cloudId, String representationName, String version) throws MCSException {

        WebTarget target = client
                .target(this.baseUrl)
                .path(DATA_SET_ASSIGNMENTS)
                .resolveTemplate(PROVIDER_ID, providerId)
                .resolveTemplate(DATA_SET_ID, dataSetId)
                .queryParam(CLOUD_ID, cloudId)
                .queryParam(REPRESENTATION_NAME, representationName)
                .queryParam(VERSION, version);

        Response response = null;

        try {
            response = target.request().delete();

            if (response.getStatus() != Status.NO_CONTENT.getStatusCode()) {
                ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
                throw MCSExceptionProvider.generateException(errorInfo);
            }
        } finally {
            closeResponse(response);
        }
    }


    /**
     * Retrieve list of existing (not deleted) cloudIds and tags from data set for specific revision.
     *
     * @param providerId         provider identifier (required)
     * @param dataSetId          data set identifier (required)
     * @param representationName name of the representation (required)
     * @param revisionName       revision name (required)
     * @param revisionProviderId revision provider id (required)
     * @param revisionTimestamp  timestamp of the searched revision which is part of the revision identifier
     * @param limit              maximum number of returned elements. Should not be greater than 10000
     * @return slice of representation cloud identifier list from data set together with tags of the revision
     * @throws MCSException on unexpected situations
     */
    public List<CloudTagsResponse> getRevisionsWithDeletedFlagSetToFalse(String providerId, String dataSetId, String representationName,
                                                                         String revisionName, String revisionProviderId,
                                                                         String revisionTimestamp, int limit) throws MCSException {
        ResultSlice<CloudTagsResponse> rs = manageResponse(ResultSlice.class, Response.Status.OK,
                () -> client.target(baseUrl)
                        .path(DATA_SET_REVISIONS_RESOURCE)
                        .resolveTemplate(PROVIDER_ID, providerId)
                        .resolveTemplate(DATA_SET_ID, dataSetId)
                        .resolveTemplate(REPRESENTATION_NAME, representationName)
                        .resolveTemplate(REVISION_NAME, revisionName)
                        .resolveTemplate(REVISION_PROVIDER_ID, revisionProviderId)
                        .queryParam(F_REVISION_TIMESTAMP, revisionTimestamp)
                        .queryParam(F_EXISTING_ONLY, true)
                        .queryParam(F_LIMIT, limit)
                        .request().get()
        );
        return rs.getResults();
    }

    @Deprecated
    public List<CloudTagsResponse> getRevisionsWithDeletedFlagSetToFalseOld(
            String providerId, String dataSetId, String representationName, String revisionName, String revisionProviderId,
            String revisionTimestamp, int limit) throws MCSException {
        WebTarget target = client.target(baseUrl)
                .path(DATA_SET_REVISIONS_RESOURCE)
                .resolveTemplate(PROVIDER_ID, providerId)
                .resolveTemplate(DATA_SET_ID, dataSetId)
                .resolveTemplate(REPRESENTATION_NAME, representationName)
                .resolveTemplate(REVISION_NAME, revisionName)
                .resolveTemplate(REVISION_PROVIDER_ID, revisionProviderId)
                .queryParam(F_REVISION_TIMESTAMP, revisionTimestamp)
                .queryParam(F_EXISTING_ONLY, true)
                .queryParam(F_LIMIT, limit);

        Response response = null;
        try {
            response = target.request().get();
            if (response.getStatus() == Status.OK.getStatusCode()) {
                return response.readEntity(ResultSlice.class).getResults();
            }
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        } finally {
            closeResponse(response);
        }
    }

    /**
     * Retrieve chunk of cloudIds and tags from data set for specific revision.
     *
     * @param providerId         provider identifier (required)
     * @param dataSetId          data set identifier (required)
     * @param representationName name of the representation (required)
     * @param revisionName       revision name (required)
     * @param revisionProviderId revision provider id (required)
     * @param revisionTimestamp  timestamp of the searched revision which is part of the revision identifier
     * @param startFrom          code pointing to the requested result slice (if equal to
     *                           null, first slice is returned)
     * @return chunk of representation cloud identifier list from data set together with tags of the revision
     * @throws MCSException on unexpected situations
     */
    @SuppressWarnings("unchecked")
    public ResultSlice<CloudTagsResponse> getDataSetRevisionsChunk(
            String providerId, String dataSetId, String representationName,
            String revisionName, String revisionProviderId, String revisionTimestamp,
            String startFrom, Integer limit) throws MCSException {

        return manageResponse(ResultSlice.class, Response.Status.OK,
                () -> client.target(baseUrl)
                        .path(DATA_SET_REVISIONS_RESOURCE)
                        .resolveTemplate(PROVIDER_ID, providerId)
                        .resolveTemplate(DATA_SET_ID, dataSetId)
                        .resolveTemplate(REPRESENTATION_NAME, representationName)
                        .resolveTemplate(REVISION_NAME, revisionName)
                        .resolveTemplate(REVISION_PROVIDER_ID, revisionProviderId)
                        .queryParam(F_REVISION_TIMESTAMP, revisionTimestamp)
                        .queryParam(F_START_FROM, startFrom)
                        .queryParam(F_LIMIT, limit != null ? limit : 0)
                        .request().get()
        );
    }

    @Deprecated
    public ResultSlice<CloudTagsResponse> getDataSetRevisionsChunkOld(
            String providerId, String dataSetId, String representationName,
            String revisionName, String revisionProviderId, String revisionTimestamp,
            String startFrom, Integer limit) throws MCSException {

        WebTarget target = client.target(baseUrl)
                .path(DATA_SET_REVISIONS_RESOURCE)
                .resolveTemplate(PROVIDER_ID, providerId)
                .resolveTemplate(DATA_SET_ID, dataSetId)
                .resolveTemplate(REPRESENTATION_NAME, representationName)
                .resolveTemplate(REVISION_NAME, revisionName)
                .resolveTemplate(REVISION_PROVIDER_ID, revisionProviderId)
                .queryParam(F_REVISION_TIMESTAMP, revisionTimestamp)
                .queryParam(F_START_FROM, startFrom);

        target = target.queryParam(F_LIMIT, limit != null ? limit : 0);

        Response response = null;
        try {
            response = target.request().get();
            if (response.getStatus() == Status.OK.getStatusCode()) {
                return response.readEntity(ResultSlice.class);
            }
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        } finally {
            closeResponse(response);
        }
    }

    /**
     * Lists cloudIds and tags from data set for specific revision.
     *
     * @param providerId         provider identifier (required)
     * @param dataSetId          data set identifier (required)
     * @param representationName name of the representation (required)
     * @param revisionName       revision name (required)
     * @param revisionProviderId revision provider id (required)
     * @param revisionTimestamp  timestamp of the searched revision which is part of the revision identifier
     * @return chunk of representation cloud identifier list from data set together with revision tags
     * @throws MCSException on unexpected situations
     */
    public List<CloudTagsResponse> getDataSetRevisions(
            String providerId, String dataSetId, String representationName,
            String revisionName, String revisionProviderId, String revisionTimestamp) throws MCSException {

        List<CloudTagsResponse> resultList = new ArrayList<>();
        ResultSlice<CloudTagsResponse> resultSlice;
        String startFrom = null;
        
        do {
            resultSlice = getDataSetRevisionsChunk(providerId, dataSetId, representationName,
                    revisionName, revisionProviderId, revisionTimestamp, startFrom, null);
            if (resultSlice == null || resultSlice.getResults() == null) {
                throw new DriverException("Getting cloud ids and revision tags: result chunk obtained but is empty.");
            }
            resultList.addAll(resultSlice.getResults());
            startFrom = resultSlice.getNextSlice();
            
        } while (resultSlice.getNextSlice() != null);
        
        return resultList;
    }

    public void useAuthorizationHeader(final String headerValue) {
        client.register(new ECloudBasicAuthFilter(headerValue));
    }

    public void close() {
        client.close();
    }

    private void closeResponse(Response response) {
        if (response != null) {
            response.close();
        }
    }

    private Form getForm(String cloudId, String representationName, String version) {
        Form form = new Form();
        form.param(CLOUD_ID, cloudId);
        form.param(REPRESENTATION_NAME, representationName);
        form.param(VERSION, version);
        return form;
    }

    protected <T> T manageResponse(Class<T> expectedResultClass, Response.Status expectedValidStatus, Supplier<Response> responseSupplier) throws MCSException {
        Response response = responseSupplier.get();
        try {
            response.bufferEntity();
            if (response.getStatus() == expectedValidStatus.getStatusCode()) {
                return readEntityByClass(expectedResultClass, response);
            }
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        } catch(MCSException | DriverException knownException) {
            throw knownException; //re-throw just created CloudException
        } catch(ProcessingException processingException) {
            String message = String.format("Could not deserialize response with statusCode: %d; message: %s",
                    response.getStatus(), response.readEntity(String.class));
            throw MCSExceptionProvider.createException(message, processingException);
        } catch(Exception otherExceptions) {
            throw MCSExceptionProvider.createException("Other client error", otherExceptions);
        } finally {
            closeResponse(response);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T readEntityByClass(Class<T> expectedResultClass, Response response) {
        if(expectedResultClass == Void.class) {
            return null;
        } else if(expectedResultClass == Boolean.class) {
            return (T)Boolean.TRUE;
        } else if(expectedResultClass == URI.class) {
            return (T)response.getLocation();
        } else {
            return response.readEntity(expectedResultClass);
        }
    }

}
