package eu.europeana.cloud.client.uis.rest;

import eu.europeana.cloud.client.uis.rest.web.StaticUrlProvider;
import eu.europeana.cloud.client.uis.rest.web.UrlProvider;
import eu.europeana.cloud.common.filter.ECloudBasicAuthFilter;
import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.UISParamConstants;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

/**
 * The REST API client for the Unique Identifier Service.
 *
 * @author Yorgos.Mamakis@ kb.nl
 */
public class UISClient {

    private static final String CLOUD_IDS_PATH = "/cloudIds";
    private static final String P_CLOUD_ID = "CLOUD_ID";
    private static final String P_PROVIDER_ID = "PROVIDER_ID";
    private static final String DATA_PROVIDERS_PATH_WITH_PROVIDER_ID = "/data-providers/{PROVIDER_ID}";
    private static final String P_LOCAL_ID = "LOCAL_ID";
    private static final String DATA_PROVIDERS_PATH = "/data-providers";
    private static final String CLOUD_IDS_PATH_WITH_CLOUD_ID = "/cloudIds/{CLOUD_ID}";

    private final Client client = ClientBuilder.newBuilder()
            .register(JacksonFeature.class)
            .build();

    private UrlProvider urlProvider;
    private static final Logger LOGGER = LoggerFactory.getLogger(UISClient.class);
    private static final int DEFAULT_CONNECT_TIMEOUT_IN_MILLIS = 20000;
    private static final int DEFAULT_READ_TIMEOUT_IN_MILLIS = 60000;

    /**
    /**
     * Creates a new instance of this class. Same as {@link #UISClient(String)}
     * but includes username and password to perform authenticated requests.
     */
    public UISClient(final String uisUrl, final String username, final String password) {
        this(uisUrl, username, password, DEFAULT_CONNECT_TIMEOUT_IN_MILLIS, DEFAULT_READ_TIMEOUT_IN_MILLIS);
    }


    public UISClient(final String uisUrl, final String username, final String password,
                     final int connectTimeoutInMillis, final int readTimeoutInMillis) {
        LOGGER.info("UISClient starting...");

        if (username != null || password != null) {
            client.register(HttpAuthenticationFeature.basic(username, password));
        }

        this.client.property(ClientProperties.CONNECT_TIMEOUT, connectTimeoutInMillis);
        this.client.property(ClientProperties.READ_TIMEOUT, readTimeoutInMillis);

        try {
            urlProvider = new StaticUrlProvider(uisUrl);
        } catch (final Exception e) {
            LOGGER.error("Error while starting UISClient... Could not start UrlProvider.. {}", e.getMessage());
        }

        LOGGER.info("UISClient started successfully.");
    }

    public UISClient(final String uisUrl, final int connectTimeoutInMillis, final int readTimeoutInMillis) {
        this(uisUrl, null, null, connectTimeoutInMillis, readTimeoutInMillis);
    }

    /**
     * Creates a new instance of this class, with a static UIS url.
     *
     * @param uisUrl The URL of some UIS instance to connect to.
     */
    public UISClient(final String uisUrl) {
        this(uisUrl, DEFAULT_CONNECT_TIMEOUT_IN_MILLIS, DEFAULT_READ_TIMEOUT_IN_MILLIS);
    }

    /**
     * Invoke the creation of a new CloudId REST call.
     *
     * @param providerId The provider Id
     * @param recordId   The record Id
     * @return The newly generated CloudId
     * @throws CloudException The generic cloud exception wrapper
     */
    public CloudId createCloudId(String providerId, String recordId)
            throws CloudException {

        Response resp = null;
        try {
            resp = client.target(urlProvider.getBaseUrl()).path(CLOUD_IDS_PATH)
                    .queryParam(UISParamConstants.Q_PROVIDER_ID, providerId)
                    .queryParam(UISParamConstants.Q_RECORD_ID, recordId).request()
                    .post(null);

            if (resp.getStatus() == Status.OK.getStatusCode()) {
                return resp.readEntity(CloudId.class);
            } else {
                throw generateException(resp.readEntity(ErrorInfo.class));
            }

        } finally {
            closeResponse(resp);
        }
    }

    /**
     * Invoke the creation of a new CloudId REST call.
     *
     * @param providerId The provider Id
     * @param recordId   The record Id
     * @param key        key of header request
     * @param value      value of header request
     * @return The newly generated CloudId
     * @throws CloudException The generic cloud exception wrapper
     */
    public CloudId createCloudId(String providerId, String recordId, String key, String value)
            throws CloudException {

        Response resp = null;
        try {
            resp = client.target(urlProvider.getBaseUrl()).path(CLOUD_IDS_PATH)
                    .queryParam(UISParamConstants.Q_PROVIDER_ID, providerId)
                    .queryParam(UISParamConstants.Q_RECORD_ID, recordId).request().header(key, value)
                    .accept(MediaType.APPLICATION_JSON)
                    .post(null);

            if (resp.getStatus() == Status.OK.getStatusCode()) {
                return resp.readEntity(CloudId.class);
            } else {
                throw generateException(resp.readEntity(ErrorInfo.class));
            }

        } finally {
            closeResponse(resp);
        }
    }

    /**
     * Invoke the creation of a new CloudId REST call.
     *
     * @param providerId The provider Id
     * @return The newly generated CloudId
     * @throws CloudException The generic cloud exception wrapper
     */
    public CloudId createCloudId(String providerId) throws CloudException {

        Response resp = null;
        try {
            resp = client.target(urlProvider.getBaseUrl()).path(CLOUD_IDS_PATH)
                    .queryParam(UISParamConstants.Q_PROVIDER_ID, providerId)
                    .request().post(null);

            if (resp.getStatus() == Status.OK.getStatusCode()) {
                return resp.readEntity(CloudId.class);
            } else {
                throw generateException(resp.readEntity(ErrorInfo.class));
            }
        } finally {
            closeResponse(resp);
        }
    }

    /**
     * Invoke the retrieval of a cloud identifier.
     *
     * @param providerId The provider Id
     * @param recordId   The record Id
     * @return The retrieved cloud Id
     * @throws CloudException The generic cloud exception wrapper
     */
    public CloudId getCloudId(String providerId, String recordId)
            throws CloudException {

        Response resp = null;
        try {
            resp = client.target(urlProvider.getBaseUrl()).path(CLOUD_IDS_PATH)
                    .queryParam(UISParamConstants.Q_PROVIDER_ID, providerId)
                    .queryParam(UISParamConstants.Q_RECORD_ID, recordId).request()
                    .get();

            if (resp.getStatus() == Status.OK.getStatusCode()) {
                return resp.readEntity(CloudId.class);
            } else {
                throw generateException(resp.readEntity(ErrorInfo.class));
            }
        } finally {
            closeResponse(resp);
        }
    }

    /**
     * Invoke the retrieval of a cloud identifier.
     *
     * @param providerId The provider Id
     * @param recordId   The record Id
     * @param key        key for head request
     * @param value      for head request
     * @return The retrieved cloud Id
     * @throws CloudException The generic cloud exception wrapper
     */
    public CloudId getCloudId(String providerId, String recordId, String key, String value)
            throws CloudException {

        Response resp = null;
        try {
            resp = client.target(urlProvider.getBaseUrl()).path(CLOUD_IDS_PATH)
                    .queryParam(UISParamConstants.Q_PROVIDER_ID, providerId)
                    .queryParam(UISParamConstants.Q_RECORD_ID, recordId).request().header(key, value)
                    .get();

            if (resp.getStatus() == Status.OK.getStatusCode()) {
                return resp.readEntity(CloudId.class);
            } else {
                throw generateException(resp.readEntity(ErrorInfo.class));
            }
        } finally {
            closeResponse(resp);
        }
    }

    /**
     * Retrieve the local identifiers associated with a cloud identifier
     *
     * @param cloudId The cloud id to search for
     * @return The List of local ids associated with the cloud id
     * @throws CloudException The generic cloud exception wrapper
     */
    @SuppressWarnings("unchecked")
    public ResultSlice<CloudId> getRecordId(String cloudId)
            throws CloudException {

        Response resp = null;
        try {
            resp = client
                    .target(urlProvider.getBaseUrl()).path(CLOUD_IDS_PATH_WITH_CLOUD_ID)
                    .resolveTemplate(P_CLOUD_ID, cloudId).request().get();

            if (resp.getStatus() == Status.OK.getStatusCode()) {
                return resp.readEntity(ResultSlice.class);
            } else {
                throw generateException(resp.readEntity(ErrorInfo.class));
            }
        } finally {
            closeResponse(resp);
        }
    }

    /**
     * Retrieve records associated with a provider.
     *
     * @param providerId The provider Id
     * @return The List of Local ids associated with a provider
     * @throws CloudException The generic cloud exception wrapper
     */
    @SuppressWarnings("unchecked")
    public ResultSlice<LocalId> getRecordIdsByProvider(String providerId)
            throws CloudException {

        Response resp = null;
        try {
            resp = client
                    .target(urlProvider.getBaseUrl()).path("/data-providers/{PROVIDER_ID}/localIds")
                    .resolveTemplate(P_PROVIDER_ID, providerId).request().get();

            if (resp.getStatus() == Status.OK.getStatusCode()) {
                return resp.readEntity(ResultSlice.class);
            } else {
                throw generateException(resp.readEntity(ErrorInfo.class));
            }
        } finally {
            closeResponse(resp);
        }
    }

    /**
     * Retrieve the Cloud ids associated with a provider.
     *
     * @param providerId The provider id
     * @return The list of cloud ids associated with the provider id
     * @throws CloudException The generic cloud exception wrapper
     */
    @SuppressWarnings("unchecked")
    public ResultSlice<CloudId> getCloudIdsByProvider(String providerId)
            throws CloudException {

        Response resp = null;
        try {
            resp = client
                    .target(urlProvider.getBaseUrl()).path("/data-providers/{PROVIDER_ID}/cloudIds")
                    .resolveTemplate(P_PROVIDER_ID, providerId).request().get();

            if (resp.getStatus() == Status.OK.getStatusCode()) {
                return resp.readEntity(ResultSlice.class);
            } else {
                throw generateException(resp.readEntity(ErrorInfo.class));
            }
        } finally {
            closeResponse(resp);
        }
    }

    /**
     * Retrieve the record ids associated with a provider with pagination.
     *
     * @param providerId    The provider id
     * @param startRecordId The local identifier to start retrieval from
     * @param limit         The maximum number of records to fetch
     * @return A list of record ids associated with the provider
     * @throws CloudException The generic cloud exception wrapper
     */
    @SuppressWarnings("unchecked")
    public ResultSlice<LocalId> getRecordIdsByProviderWithPagination(
            String providerId, String startRecordId, int limit)
            throws CloudException {

        Response resp = null;
        try {
            resp = client
                    .target(urlProvider.getBaseUrl()).path("/data-providers/{PROVIDER_ID}/localIds")
                    .resolveTemplate(P_PROVIDER_ID, providerId)
                    .queryParam(UISParamConstants.Q_FROM, startRecordId)
                    .queryParam(UISParamConstants.Q_LIMIT, limit).request().get();

            if (resp.getStatus() == Status.OK.getStatusCode()) {
                return resp.readEntity(ResultSlice.class);
            } else {
                throw generateException(resp.readEntity(ErrorInfo.class));
            }
        } finally {
            closeResponse(resp);
        }
    }

    /**
     * Retrieve the cloud ids associated with a provider with pagination.
     *
     * @param providerId    The provider id
     * @param startRecordId The local identifier to start retrieval from
     * @param limit         The maximum number of records to fetch
     * @return A list of cloud ids associated with the provider
     * @throws CloudException The generic cloud exception wrapper
     */
    @SuppressWarnings("unchecked")
    public ResultSlice<CloudId> getCloudIdsByProviderWithPagination(
            String providerId, String startRecordId, int limit)
            throws CloudException {

        Response resp = null;
        try {
            resp = client
                    .target(urlProvider.getBaseUrl()).path("/data-providers/{PROVIDER_ID}/cloudIds")
                    .resolveTemplate(P_PROVIDER_ID, providerId)
                    .queryParam(UISParamConstants.Q_FROM, startRecordId)
                    .queryParam(UISParamConstants.Q_LIMIT, limit).request().get();

            if (resp.getStatus() == Status.OK.getStatusCode()) {
                return resp.readEntity(ResultSlice.class);
            } else {
                throw generateException(resp.readEntity(ErrorInfo.class));
            }
        } finally {
            closeResponse(resp);
        }
    }

    /**
     * Create a mapping between a cloud id and provider and record id.
     *
     * @param cloudId    The cloud id
     * @param providerId The provider id
     * @param recordId   The record id
     * @return A confirmation that the mapping has been created
     * @throws CloudException The generic cloud exception wrapper
     */
    public boolean createMapping(String cloudId, String providerId,
                                 String recordId) throws CloudException {

        Response resp = null;
        try {
            resp = client
                    .target(urlProvider.getBaseUrl()).path("/data-providers/{PROVIDER_ID}/cloudIds/{CLOUD_ID}")
                    .resolveTemplate(P_PROVIDER_ID, providerId)
                    .resolveTemplate(P_CLOUD_ID, cloudId)
                    .queryParam(UISParamConstants.Q_RECORD_ID, recordId).request()
                    .post(null);

            if (resp.getStatus() == Status.OK.getStatusCode()) {
                return true;
            } else {
                throw generateException(resp.readEntity(ErrorInfo.class));
            }
        } finally {
            closeResponse(resp);
        }
    }


    /**
     * Create a mapping between a cloud id and provider and record id.
     *
     * @param cloudId    The cloud id
     * @param providerId The provider id
     * @param recordId   The record id
     * @param key        key of header request
     * @param value      value of header request
     * @return A confirmation that the mapping has been created
     * @throws CloudException The generic cloud exception wrapper
     */
    public boolean createMapping(String cloudId, String providerId,
                                 String recordId, String key, String value) throws CloudException {

        Response resp = null;
        try {
            resp = client
                    .target(urlProvider.getBaseUrl()).path("/data-providers/{PROVIDER_ID}/cloudIds/{CLOUD_ID}")
                    .resolveTemplate(P_PROVIDER_ID, providerId)
                    .resolveTemplate(P_CLOUD_ID, cloudId)
                    .queryParam(UISParamConstants.Q_RECORD_ID, recordId).request().header(key, value)
                    .post(null);

            if (resp.getStatus() == Status.OK.getStatusCode()) {
                return true;
            } else {
                throw generateException(resp.readEntity(ErrorInfo.class));
            }
        } finally {
            closeResponse(resp);
        }
    }

    /**
     * Remove the association of a record id to a cloud id.
     *
     * @param providerId The provider id to use
     * @param recordId   The record id to use
     * @return A confirmation that the mapping has removed correctly
     * @throws CloudException The generic cloud exception wrapper
     */
    public boolean removeMappingByLocalId(String providerId, String recordId)
            throws CloudException {

        Response resp = null;
        try {
            resp = client
                    .target(urlProvider.getBaseUrl()).path("/data-providers/{PROVIDER_ID}/localIds/{LOCAL_ID}")
                    .resolveTemplate(P_PROVIDER_ID, providerId)
                    .resolveTemplate(P_LOCAL_ID, recordId).request().delete();

            if (resp.getStatus() == Status.OK.getStatusCode()) {
                return true;
            } else {
                throw generateException(resp.readEntity(ErrorInfo.class));
            }
        } finally {
            closeResponse(resp);
        }
    }

    /**
     * Delete a cloud id and all its mapped record ids.
     *
     * @param cloudId The cloud id to remove
     * @return A confirmation message that the mappings have been removed
     * correctly
     * @throws CloudException The generic cloud exception wrapper
     */
    public boolean deleteCloudId(String cloudId) throws CloudException {

        Response resp = null;
        try {
            resp = client
                    .target(urlProvider.getBaseUrl()).path(CLOUD_IDS_PATH_WITH_CLOUD_ID)
                    .resolveTemplate(P_CLOUD_ID, cloudId).request().delete();

            if (resp.getStatus() == Status.OK.getStatusCode()) {
                return true;
            } else {
                throw generateException(resp.readEntity(ErrorInfo.class));
            }
        } finally {
            closeResponse(resp);
        }
    }

    /**
     * Create a data provider.
     *
     * @param providerId The data provider Id
     * @param dp         The data provider properties
     * @return A URL that points to the data provider
     * @throws CloudException
     */
    public String createProvider(String providerId, DataProviderProperties dp)
            throws CloudException {

        Response resp = null;
        try {
            resp = client
                    .target(urlProvider.getBaseUrl()).path(DATA_PROVIDERS_PATH)
                    .queryParam(UISParamConstants.Q_PROVIDER, providerId).request()
                    .post(Entity.json(dp));

            if (resp.getStatus() == Status.CREATED.getStatusCode()) {
                return resp.toString();
            } else {
                throw generateException(resp.readEntity(ErrorInfo.class));
            }
        } finally {
            closeResponse(resp);
        }
    }

    /**
     * Update a Data Provider.
     *
     * @param providerId The provider to update
     * @param dp         The data provider properties
     * @return True if successful, false else
     * @throws CloudException
     */
    public boolean updateProvider(String providerId, DataProviderProperties dp)
            throws CloudException {

        Response resp = null;
        try {
            resp = client
                    .target(urlProvider.getBaseUrl()).path(DATA_PROVIDERS_PATH_WITH_PROVIDER_ID)
                    .resolveTemplate(P_PROVIDER_ID, providerId).request()
                    .put(Entity.json(dp));

            if (resp.getStatus() == Status.NO_CONTENT.getStatusCode()) {
                return true;
            } else {
                throw generateException(resp.readEntity(ErrorInfo.class));
            }
        } finally {
            closeResponse(resp);
        }
    }

    /**
     * Get data providers
     *
     * @param from The record to start from
     * @return A predefined number of data providers
     * @throws CloudException
     */
    @SuppressWarnings("unchecked")
    public ResultSlice<DataProvider> getDataProviders(String from)
            throws CloudException {

        Response resp = null;
        try {
            resp = client
                    .target(urlProvider.getBaseUrl()).path(DATA_PROVIDERS_PATH)
                    .queryParam(UISParamConstants.Q_FROM, from).request().get();

            if (resp.getStatus() == Status.OK.getStatusCode()) {
                return resp.readEntity(ResultSlice.class);
            } else {
                throw generateException(resp.readEntity(ErrorInfo.class));
            }
        } finally {
            closeResponse(resp);
        }
    }

    /**
     * Retrieve a selected data provider.
     *
     * @param providerId The provider id to retrieve
     * @return The Data provider that corresponds to the selected id
     * @throws CloudException
     */
    public DataProvider getDataProvider(String providerId)
            throws CloudException {

        Response resp = null;
        try {
            resp = client
                    .target(urlProvider.getBaseUrl()).path(DATA_PROVIDERS_PATH_WITH_PROVIDER_ID)
                    .resolveTemplate(P_PROVIDER_ID, providerId).request().get();

            if (resp.getStatus() == Status.OK.getStatusCode()) {
                return resp.readEntity(DataProvider.class);
            } else {
                throw generateException(resp.readEntity(ErrorInfo.class));
            }
        } finally {
            closeResponse(resp);
        }
    }

    /**
     * Generates the exception to be returned to the client.
     *
     * @param e The error info that was generated
     * @return A CloudException that wraps the original exception
     */
    public CloudException generateException(ErrorInfo e) {
        IdentifierErrorTemplate error = IdentifierErrorTemplate.valueOf(e
                .getErrorCode());
        LOGGER.error(e.getDetails());
        return new CloudException(e.getErrorCode(), error.getException(e));
    }

    private void closeResponse(Response response) {
        if (response != null) {
            response.close();
        }
    }

    /**
     * Client will use provided authorization header for all requests;
     *
     * @param headerValue authorization header value
     * @return
     */
    public void useAuthorizationHeader(final String headerValue) {
        client.register(new ECloudBasicAuthFilter(headerValue));
    }

    @Override
    protected void finalize() throws Throwable {
        client.close();
    }

    public void close() {
        client.close();
    }
}
