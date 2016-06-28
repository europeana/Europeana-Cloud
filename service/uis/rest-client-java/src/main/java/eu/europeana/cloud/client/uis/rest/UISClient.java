package eu.europeana.cloud.client.uis.rest;

import eu.europeana.cloud.client.uis.rest.web.DynamicUrlProvider;
import eu.europeana.cloud.client.uis.rest.web.StaticUrlProvider;
import eu.europeana.cloud.client.uis.rest.web.UrlProvider;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.UISParamConstants;
import eu.europeana.cloud.service.coordination.provider.ServiceProvider;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.client.filter.HttpBasicAuthFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;

/**
 * The REST API client for the Unique Identifier Service.
 *
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
public class UISClient {

    private Client client = JerseyClientBuilder.newClient();

    private UrlProvider urlProvider;

    private static final Logger LOGGER = LoggerFactory.getLogger(UISClient.class);

    /**
     * Creates a new instance of this class.
     *
     * Since no URL is provided, 
	 * default properties will be read from the properties file.
	 * 
     */
    public UISClient() {
        LOGGER.info("UISClient starting... no UIS-URL provided.");

        try {
            urlProvider = new DynamicUrlProvider();
        } catch (final IOException e) {
            LOGGER.error("Error while starting UISClient... Could not start UrlProvider.. {}", e.getMessage());
        }

        LOGGER.info("UISClient started successfully.");
    }

    /**
     * Creates a new instance of this class.
     *
     * Since no URL is provided, default properties will be read from the properties file.
     * Same as {@link #UISClient()} but includes username and password to
     * perform authenticated requests.
     */
    public UISClient(final String username, final String password) {
        LOGGER.info("UISClient starting... no UIS-URL provided.");

        try {
            urlProvider = new DynamicUrlProvider();
        } catch (final IOException e) {
            LOGGER.error("Error while starting UISClient... Could not start UrlProvider.. {}", e.getMessage());
        }

        LOGGER.info("UISClient started successfully.");
    }


    /**
     * Creates a new instance of this class. 
     * UIS url is dynamically provided from the specified {@link ServiceProvider}
     *
     * @param uisProvider
     */
    public UISClient(final ServiceProvider uisProvider) {
        LOGGER.info("UISClient starting...");

        try {
            urlProvider = new DynamicUrlProvider(uisProvider);
        } catch (final Exception e) {
            LOGGER.error("Error while starting UISClient... Could not start UrlProvider.. {}", e.getMessage());
        }

        LOGGER.info("UISClient started successfully.");
    }

    /**
     * Creates a new instance of this class. Same as
     * {@link #UISClient(ServiceProvider)} but includes username and password to
     * perform authenticated requests.
     *
     */
    public UISClient(final ServiceProvider uisProvider, final String username, final String password) {
        LOGGER.info("UISClient starting...");

        client.register(new HttpBasicAuthFilter(username, password));

        try {
            urlProvider = new DynamicUrlProvider(uisProvider);
        } catch (final Exception e) {
            LOGGER.error("Error while starting UISClient... Could not start UrlProvider.. {}", e.getMessage());
        }

        LOGGER.info("UISClient started successfully.");
    }

    /**
     * Creates a new instance of this class. Same as {@link #UISClient(String)}
     * but includes username and password to perform authenticated requests.
     *
     */
    public UISClient(final String uisUrl, final String username, final String password) {
        LOGGER.info("UISClient starting...");

        client.register(new HttpBasicAuthFilter(username, password));

        try {
            urlProvider = new StaticUrlProvider(uisUrl);
        } catch (final Exception e) {
            LOGGER.error("Error while starting UISClient... Could not start UrlProvider.. {}", e.getMessage());
        }

        LOGGER.info("UISClient started successfully.");
    }

    /**
     * Creates a new instance of this class, with a static UIS url.
     *
     * @param uisUrl The URL of some UIS instance to connect to.
     */
    public UISClient(final String uisUrl) {
        LOGGER.info("UISClient starting...");

        try {
            urlProvider = new StaticUrlProvider(uisUrl);
        } catch (final Exception e) {
            LOGGER.error("Error while starting UISClient... Could not start UrlProvider.. {}", e.getMessage());
        }

        LOGGER.info("UISClient started successfully.");
    }

    /**
     * Invoke the creation of a new CloudId REST call.
     *
     * @param providerId The provider Id
     * @param recordId The record Id
     * @return The newly generated CloudId
     * @throws CloudException The generic cloud exception wrapper
     */
    public CloudId createCloudId(String providerId, String recordId)
            throws CloudException {

        Response resp = null;
        try {
            resp = client.target(urlProvider.getBaseUrl() + "/cloudIds")
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
     * @return The newly generated CloudId
     * @throws CloudException The generic cloud exception wrapper
     */
    public CloudId createCloudId(String providerId) throws CloudException {

        Response resp = null;
        try {
            resp = client.target(urlProvider.getBaseUrl() + "/cloudIds")
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
     * @param recordId The record Id
     * @return The retrieved cloud Id
     * @throws CloudException The generic cloud exception wrapper
     */
    public CloudId getCloudId(String providerId, String recordId)
            throws CloudException {

        Response resp = null;
        try {
            resp = client.target(urlProvider.getBaseUrl() + "/cloudIds")
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
                    .target(urlProvider.getBaseUrl() + "/cloudIds/{CLOUD_ID}")
                    .resolveTemplate("CLOUD_ID", cloudId).request().get();

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
                    .target(urlProvider.getBaseUrl()
                            + "/data-providers/{PROVIDER_ID}/localIds")
                    .resolveTemplate("PROVIDER_ID", providerId).request().get();

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
                    .target(urlProvider.getBaseUrl()
                            + "/data-providers/{PROVIDER_ID}/cloudIds")
                    .resolveTemplate("PROVIDER_ID", providerId).request().get();

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
     * @param providerId The provider id
     * @param startRecordId The local identifier to start retrieval from
     * @param limit The maximum number of records to fetch
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
                    .target(urlProvider.getBaseUrl()
                            + "/data-providers/{PROVIDER_ID}/localIds")
                    .resolveTemplate("PROVIDER_ID", providerId)
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
     * @param providerId The provider id
     * @param startRecordId The local identifier to start retrieval from
     * @param limit The maximum number of records to fetch
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
                    .target(urlProvider.getBaseUrl()
                            + "/data-providers/{PROVIDER_ID}/cloudIds")
                    .resolveTemplate("PROVIDER_ID", providerId)
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
     * @param cloudId The cloud id
     * @param providerId The provider id
     * @param recordId The record id
     * @return A confirmation that the mapping has been created
     * @throws CloudException The generic cloud exception wrapper
     */
    public boolean createMapping(String cloudId, String providerId,
            String recordId) throws CloudException {

        Response resp = null;
        try {
            resp = client
                    .target(urlProvider.getBaseUrl()
                            + "/data-providers/{PROVIDER_ID}/cloudIds/{CLOUD_ID}")
                    .resolveTemplate("PROVIDER_ID", providerId)
                    .resolveTemplate("CLOUD_ID", cloudId)
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
     * Remove the association of a record id to a cloud id.
     *
     * @param providerId The provider id to use
     * @param recordId The record id to use
     * @return A confirmation that the mapping has removed correctly
     * @throws CloudException The generic cloud exception wrapper
     */
    public boolean removeMappingByLocalId(String providerId, String recordId)
            throws CloudException {

        Response resp = null;
        try {
            resp = client
                    .target(urlProvider.getBaseUrl()
                            + "/data-providers/{PROVIDER_ID}/localIds/{LOCAL_ID}")
                    .resolveTemplate("PROVIDER_ID", providerId)
                    .resolveTemplate("LOCAL_ID", recordId).request().delete();

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
                    .target(urlProvider.getBaseUrl() + "/cloudIds/{CLOUD_ID}")
                    .resolveTemplate("CLOUD_ID", cloudId).request().delete();

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
     * @param dp The data provider properties
     * @return A URL that points to the data provider
     * @throws CloudException
     */
    public String createProvider(String providerId, DataProviderProperties dp)
            throws CloudException {

        Response resp = null;
        try {
            resp = client
                    .target(urlProvider.getBaseUrl() + "/data-providers")
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
     * @param dp The data provider properties
     * @return True if successful, false else
     * @throws CloudException
     */
    public boolean updateProvider(String providerId, DataProviderProperties dp)
            throws CloudException {

        Response resp = null;
        try {
            resp = client
                    .target(urlProvider.getBaseUrl()
                            + "/data-providers/{PROVIDER_ID}")
                    .resolveTemplate("PROVIDER_ID", providerId).request()
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
                    .target(urlProvider.getBaseUrl() + "/data-providers")
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
                    .target(urlProvider.getBaseUrl()
                            + "/data-providers/{PROVIDER_ID}")
                    .resolveTemplate("PROVIDER_ID", providerId).request().get();

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

    @Override
    protected void finalize() throws Throwable {
        client.close();
    }
}
