package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.filter.ECloudBasicAuthFilter;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.CloudVersionRevisionResponse;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.*;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static eu.europeana.cloud.common.web.ParamConstants.*;

/**
 * Client for managing datasets in MCS.
 */
public class DataSetServiceClient extends MCSClient {

    private final Client client = ClientBuilder.newClient();
    private static final Logger logger = LoggerFactory.getLogger(DataSetServiceClient.class);

    //data-providers/{DATAPROVIDER}/data-sets
    private static final String dataSetsPath; // = ParamConstants.PROVIDERS + "/{" + ParamConstants.P_PROVIDER + "}/" + ParamConstants.DATASETS;
    //data-providers/{DATAPROVIDER}/data-sets/{DATASET}
    private static final String dataSetPath; // = dataSetsPath + "/{" + ParamConstants.P_DATASET + "}";
    //data-providers/{DATAPROVIDER}/data-sets/{DATASET}/assignments
    private static final String assignmentsPath; // = dataSetPath + "/" + ParamConstants.ASSIGNMENTS;
    //data-providers/{DATAPROVIDER}/data-sets/{DATASET}/representations/{REPRESENTATIONNAME}/revisions/{REVISIONID}"
    private static final String dataSetRevisionsPath;

    //data-providers/{DATAPROVIDER}/data-sets/{DATASET}/representations/{REPRESENTATIONNAME}
    private static final String representationsPath; // = dataSetPath + "/representations/{" + ParamConstants.P_REPRESENTATIONNAME + "}";

    private static final String latelyTaggedRecordsPath;

    private static final String revisionAndRepresentationPath;

    static {
        final StringBuilder builder = new StringBuilder();

        builder.append(ParamConstants.PROVIDERS);
        builder.append("/");
        builder.append("{");
        builder.append(P_PROVIDER);
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

        final StringBuilder dataSetRevisionsPathBuilder = new StringBuilder(dataSetPath);
        dataSetRevisionsPathBuilder
                .append("/")
                .append(ParamConstants.REPRESENTATIONS)
                .append("/{")
                .append(ParamConstants.P_REPRESENTATIONNAME)
                .append("}/")
                .append(ParamConstants.REVISIONS)
                .append("/{")
                .append(ParamConstants.P_REVISION_NAME)
                .append("}/")
                .append(ParamConstants.REVISION_PROVIDER)
                .append("/{")
                .append(ParamConstants.P_REVISION_PROVIDER_ID)
                .append("}");
        dataSetRevisionsPath = dataSetRevisionsPathBuilder.toString();
        representationsPath = dataSetPath + "/" + ParamConstants.REPRESENTATIONS + "/{" + ParamConstants.P_REPRESENTATIONNAME + "}";
        latelyTaggedRecordsPath = dataSetPath + "/latelyRevisionedVersion";
        revisionAndRepresentationPath = dataSetPath + "/revision" + "/{" + P_REVISION_NAME + "}/" + REVISION_PROVIDER + "/{" + P_REVISION_PROVIDER_ID + "}/" + REPRESENTATIONS + "/{" + P_REPRESENTATIONNAME + "}";
    }


    /**
     * Creates instance of DataSetServiceClient.
     *
     * @param baseUrl URL of the MCS Rest Service
     */
    public DataSetServiceClient(String baseUrl) {
        super(baseUrl);

    }

    /**
     * Creates instance of DataSetServiceClient. Same as {@link #DataSetServiceClient(String)}
     * but includes username and password to perform authenticated requests.
     *
     * @param baseUrl URL of the MCS Rest Service
     */
    public DataSetServiceClient(String baseUrl, final String username, final String password) {
        this(baseUrl);
        client
                .register(HttpAuthenticationFeature.basicBuilder().credentials(username, password).build());
    }

    /**
     * Returns chunk of data sets list of specified provider.
     * <p/>
     * This method returns the chunk specified by <code>startFrom</code>
     * parameter. If parameter is <code>null</code>, the first chunk is
     * returned. You can use {@link ResultSlice#getNextSlice()} of returned
     * result to obtain <code>startFrom</code> value to get the next chunk, etc;
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
    public ResultSlice<DataSet> getDataSetsForProviderChunk(String providerId, String startFrom)
            throws MCSException {

        WebTarget target = client.target(this.baseUrl).path(dataSetsPath)
                .resolveTemplate(P_PROVIDER, providerId);

        if (startFrom != null) {
            target = target.queryParam(ParamConstants.F_START_FROM, startFrom);
        }

        return prepareResultSliceResponse(target);
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
    public List<DataSet> getDataSetsForProvider(String providerId)
            throws MCSException {

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
    public URI createDataSet(String providerId, String dataSetId, String description)
            throws ProviderNotExistsException, DataSetAlreadyExistsException, MCSException {

        WebTarget target = client.target(this.baseUrl).path(dataSetsPath)
                .resolveTemplate(P_PROVIDER, providerId);

        Form form = new Form();
        form.param(ParamConstants.F_DATASET, dataSetId);
        form.param(ParamConstants.F_DESCRIPTION, description);

        Response response = null;

        try {
            response = target.request().post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));

            if (response.getStatus() == Status.CREATED.getStatusCode()) {
                return response.getLocation();
            }

            //TODO this does not function correctly,
            //details are filled with "MessageBodyReader not found for media type=text/html; 
            //charset=utf-8, type=class eu.europeana.cloud.common.response.ErrorInfo, 
            //genericType=class eu.europeana.cloud.common.response.ErrorInfo."
            //simple strings like 'adsfd' get entitised correctly
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
    public ResultSlice<Representation> getDataSetRepresentationsChunk(String providerId, String dataSetId,
                                                                      String startFrom)
            throws DataSetNotExistsException, MCSException {
        WebTarget target = client.target(this.baseUrl).path(dataSetPath)
                .resolveTemplate(P_PROVIDER, providerId)
                .resolveTemplate(ParamConstants.P_DATASET, dataSetId);

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
    public List<Representation> getDataSetRepresentations(String providerId, String dataSetId)
            throws DataSetNotExistsException, MCSException {

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
    public void updateDescriptionOfDataSet(String providerId, String dataSetId, String description)
            throws DataSetNotExistsException, MCSException {
        WebTarget target = client.target(this.baseUrl).path(dataSetPath)
                .resolveTemplate(P_PROVIDER, providerId)
                .resolveTemplate(ParamConstants.P_DATASET, dataSetId);

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
    public void deleteDataSet(String providerId, String dataSetId)
            throws DataSetNotExistsException, MCSException {
        WebTarget target = client.target(this.baseUrl).path(dataSetPath)
                .resolveTemplate(P_PROVIDER, providerId)
                .resolveTemplate(ParamConstants.P_DATASET, dataSetId);

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
     * {@link Representation#version} filled.
     *
     * @param providerId         provider identifier (required)
     * @param dataSetId          data set identifier (required)
     * @param cloudId            cloudId of the record (required)
     * @param representationName name of the representation (required)
     * @param versionId          version of representation; if not provided, latest
     *                           persistent version will be assigned to data set
     * @throws DataSetNotExistsException        if data set does not exist
     * @throws RepresentationNotExistsException if no such representation exists
     * @throws MCSException                     on unexpected situations
     */
    public void assignRepresentationToDataSet(String providerId, String dataSetId, String cloudId,
                                              String representationName, String versionId)
            throws DataSetNotExistsException, RepresentationNotExistsException, MCSException {
        WebTarget target = client.target(this.baseUrl).path(assignmentsPath)
                .resolveTemplate(P_PROVIDER, providerId)
                .resolveTemplate(ParamConstants.P_DATASET, dataSetId);

        Form form = new Form();
        form.param(ParamConstants.F_CLOUDID, cloudId);
        form.param(ParamConstants.F_REPRESENTATIONNAME, representationName);
        form.param(ParamConstants.F_VER, versionId);

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
     * Unassigns representation from data set.
     * <p/>
     * If representation was not assigned to data set, nothing happens. If
     * representation does not exist, nothing happens.
     *
     * @param providerId         provider identifier (required)
     * @param dataSetId          data set identifier (required)
     * @param cloudId            cloudId of the record (required)
     * @param representationName name of the representation (required)
     * @throws DataSetNotExistsException if data set does not exist
     * @throws MCSException              on unexpected situations
     */
    public void unassignRepresentationFromDataSet(String providerId, String dataSetId, String cloudId,
                                                  String representationName, String representationVersion)
            throws DataSetNotExistsException, MCSException {

        WebTarget target = client.target(this.baseUrl).path(assignmentsPath)
                .resolveTemplate(P_PROVIDER, providerId)
                .resolveTemplate(ParamConstants.P_DATASET, dataSetId).queryParam(ParamConstants.F_CLOUDID, cloudId)
                .queryParam(ParamConstants.F_REPRESENTATIONNAME, representationName)
                .queryParam(ParamConstants.F_VER, representationVersion);

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
     * Retrieve chunk of cloudIds and tags from data set for specific revision.
     *
     * @param providerId         provider identifier (required)
     * @param dataSetId          data set identifier (requred)
     * @param representationName name of the representation (required)
     * @param revisionName       revision naem (required)
     * @param revisionProviderId revision provider id (required)
     * @param revisionTimestamp  timestamp of the searched revision which is part of the revision identifier
     * @param startFrom          code pointing to the requested result slice (if equal to
     *                           null, first slice is returned)
     * @return chunk of representation cloud identifier list from data set together with tags of the revision
     * @throws MCSException on unexpected situations
     */
    public ResultSlice<CloudTagsResponse> getDataSetRevisionsChunk(String providerId, String dataSetId,
                                                                   String representationName, String revisionName,
                                                                   String revisionProviderId, String revisionTimestamp, String startFrom, Integer limit)
            throws MCSException {

        WebTarget target = client.target(baseUrl)
                .path(dataSetRevisionsPath)
                .resolveTemplate(P_PROVIDER, providerId)
                .resolveTemplate(P_DATASET, dataSetId)
                .resolveTemplate(P_REPRESENTATIONNAME, representationName)
                .resolveTemplate(P_REVISION_NAME, revisionName)
                .resolveTemplate(P_REVISION_PROVIDER_ID, revisionProviderId)
                .queryParam(F_REVISION_TIMESTAMP, revisionTimestamp)
                .queryParam(F_START_FROM, startFrom);

        if (limit != null) {
            target = target.queryParam(F_LIMIT, limit);
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
            closeResponse(response);
        }
    }

    /**
     * Lists cloudIds and tags from data set for specific revision.
     *
     * @param providerId         provider identifier (required)
     * @param dataSetId          data set identifier (requred)
     * @param representationName name of the representation (required)
     * @param revisionName       revision naem (required)
     * @param revisionProviderId revision provider id (required)
     * @param revisionTimestamp  timestamp of the searched revision which is part of the revision identifier
     * @return chunk of representation cloud identifier list from data set together with revision tags
     * @throws MCSException on unexpected situations
     */
    public List<CloudTagsResponse> getDataSetRevisions(String providerId, String dataSetId,
                                                       String representationName, String revisionName,
                                                       String revisionProviderId, String revisionTimestamp)
            throws MCSException {

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

    private void closeResponse(Response response) {
        if (response != null) {
            response.close();
        }
    }

    public void useAuthorizationHeader(final String headerValue) {
        client.register(new ECloudBasicAuthFilter(headerValue));

    }


    @Override
    protected void finalize() throws Throwable {
        client.close();
    }


    /**
     * Returns chunk of cloud identifiers list of specified provider in specified data set having specific representation name
     * and published revision created after the specified date.
     * <p/>
     * This method returns the chunk specified by <code>startFrom</code>
     * parameter. If parameter is <code>null</code>, the first chunk is
     * returned. You can use {@link ResultSlice#getNextSlice()} of returned
     * result to obtain <code>startFrom</code> value to get the next chunk, etc;
     * if
     * {@link eu.europeana.cloud.common.response.ResultSlice#getNextSlice()}<code>==null</code>
     * in returned result it means it is the last slice.
     * <p/>
     *
     * @param dataSetId          data set identifier (required)
     * @param providerId         provider identifier (required)
     * @param representationName name of the representation (required)
     * @param dateFrom           starting date (required)
     * @param tag                tag of revision, must be published, other are not supported yet (required)
     * @param startFrom          code pointing to the requested result slice (if equal to
     *                           null, first slice is returned)
     * @return chunk of cloud identifiers list of specified provider in specified data set having specific representation name
     * and published revision created after the specified date
     * @throws MCSException on unexpected situations
     */
    public ResultSlice<CloudVersionRevisionResponse> getDataSetCloudIdsByRepresentationChunk(String dataSetId, String providerId, String representationName, String dateFrom, String tag, String startFrom)
            throws MCSException {


        WebTarget target = client.target(this.baseUrl).path(representationsPath)
                .resolveTemplate(ParamConstants.P_PROVIDER, providerId)
                .resolveTemplate(ParamConstants.P_DATASET, dataSetId)
                .resolveTemplate(ParamConstants.P_REPRESENTATIONNAME, representationName)
                .queryParam(ParamConstants.F_DATE_FROM, dateFrom)
                .queryParam(ParamConstants.F_TAG, tag);

        if (startFrom != null) {
            target = target.queryParam(ParamConstants.F_START_FROM, startFrom);
        }

        return prepareResultSliceResponse(target);

    }

    /**
     * Lists all cloud identifiers from data provider's data set having representation name and published revision added after specified date.
     * <p/>
     *
     * @param dataSetId          data set identifier (required)
     * @param providerId         provider identifier (required)
     * @param representationName name of the representation (required)
     * @param dateFrom           starting date (required)
     * @param tag                tag of revision, must be published, other are not supported yet (required)
     * @return list of all data sets of specified provider (empty if provider
     * does not exist)
     * @throws MCSException on unexpected situations
     */
    public List<CloudVersionRevisionResponse> getDataSetCloudIdsByRepresentation(String dataSetId, String providerId, String representationName, String dateFrom, String tag)
            throws MCSException {

        List<CloudVersionRevisionResponse> resultList = new ArrayList<>();
        ResultSlice resultSlice;
        String startFrom = null;

        do {
            resultSlice = getDataSetCloudIdsByRepresentationChunk(dataSetId, providerId, representationName, dateFrom, tag, startFrom);
            if (resultSlice == null || resultSlice.getResults() == null) {
                throw new DriverException("Getting cloud identifiers from data set: result chunk obtained but is empty.");
            }
            resultList.addAll(resultSlice.getResults());
            startFrom = resultSlice.getNextSlice();

        } while (resultSlice.getNextSlice() != null);

        return resultList;
    }

    /**
     * Returns chunk of the latest cloud identifier,revision timestamp that belong to data set of a specified provider for a specific representation and revision.
     *
     * @param dataSetId          data set identifier
     * @param providerId         provider identifier
     * @param revisionProvider   revision provider
     * @param revisionName       revision name
     * @param representationName representation name
     * @param startFrom          identifier to the requested result slice (cloudId) (if equal to
     *                           null, first slice is returned)
     * @return chunk of the latest cloud identifier,revision timestamp that belong to data set of a specified provider for a specific representation and revision.
     * @throws MCSException on  unexpected situations
     */

    public ResultSlice<CloudIdAndTimestampResponse> getLatestDataSetCloudIdByRepresentationAndRevisionChunk(String dataSetId, String providerId, String revisionProvider, String revisionName, String representationName, Boolean isDeleted, String startFrom)
            throws MCSException {


        WebTarget target = client.target(this.baseUrl).path(revisionAndRepresentationPath)
                .resolveTemplate(P_PROVIDER, providerId)
                .resolveTemplate(P_REVISION_NAME, revisionName)
                .resolveTemplate(P_REVISION_PROVIDER_ID, revisionProvider)
                .resolveTemplate(P_DATASET, dataSetId)
                .resolveTemplate(P_REPRESENTATIONNAME, representationName);


        if (startFrom != null) {
            target = target.queryParam(F_START_FROM, startFrom);
        }
        if (isDeleted != null) {
            target = target.queryParam(IS_DELETED, isDeleted);
        }

        return prepareResultSliceResponse(target);
    }

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
     * Returns list of the latest cloud identifier,revision timestamp that belong to data set of a specified provider for a specific representation and revision
     * <p>
     * <p/>
     *
     * @param dataSetId          data set identifier
     * @param providerId         provider identifier
     * @param revisionProvider   revision provider
     * @param revisionName       revision name
     * @param representationName representation name
     * @param isDeleted          marked deleted
     * @throws MCSException on unexpected situations
     */
    public List<CloudIdAndTimestampResponse> getLatestDataSetCloudIdByRepresentationAndRevision(String dataSetId, String providerId, String revisionProvider, String revisionName, String representationName, Boolean isDeleted)
            throws MCSException {

        List<CloudIdAndTimestampResponse> resultList = new ArrayList<>();
        ResultSlice resultSlice;
        String startFrom = null;

        do {
            resultSlice = getLatestDataSetCloudIdByRepresentationAndRevisionChunk(dataSetId, providerId, revisionProvider, revisionName, representationName, isDeleted, startFrom);
            if (resultSlice == null || resultSlice.getResults() == null) {
                throw new DriverException("Getting cloud identifiers from data set: result chunk obtained but is empty.");
            }
            resultList.addAll(resultSlice.getResults());
            startFrom = resultSlice.getNextSlice();

        } while (resultSlice.getNextSlice() != null);

        return resultList;
    }


    /**
     * Gives the versionId of specified representation that has the newest revision (by revision timestamp) with given name.
     *
     * @param dataSetId          dataset identifier
     * @param providerId         dataset owner
     * @param cloudId            representation cloud identifier
     * @param representationName representation name
     * @param revisionName       revision name
     * @param revisionProviderId revision owner
     * @return version identifier of representation
     * @throws DataSetNotExistsException
     */
    public String getLatelyTaggedRecords(String dataSetId, String providerId, String cloudId, String representationName, String revisionName, String revisionProviderId)
            throws MCSException {

        WebTarget target = client.target(this.baseUrl).path(latelyTaggedRecordsPath)
                .resolveTemplate(ParamConstants.P_PROVIDER, providerId)
                .resolveTemplate(ParamConstants.P_DATASET, dataSetId)
                .queryParam(ParamConstants.F_CLOUDID, cloudId)
                .queryParam(ParamConstants.F_REPRESENTATIONNAME, representationName)
                .queryParam(ParamConstants.F_REVISION_NAME, revisionName)
                .queryParam(ParamConstants.F_REVISION_PROVIDER_ID, revisionProviderId);

        Response response = null;
        try {
            response = target.request().get();
            if (response.getStatus() == Status.OK.getStatusCode()) {
                return response.readEntity(String.class);
            } else if (response.getStatus() == Status.NO_CONTENT.getStatusCode()) {
                return null;
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

}
