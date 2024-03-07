package eu.europeana.cloud.client.uis.rest;

import eu.europeana.cloud.client.uis.rest.web.StaticUrlProvider;
import eu.europeana.cloud.client.uis.rest.web.UrlProvider;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.model.LocalId;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.UISParamConstants;
import eu.europeana.cloud.service.uis.status.IdentifierErrorTemplate;
import java.util.function.Supplier;

import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import lombok.Getter;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The REST API client for the Unique Identifier Service.
 */
public class UISClient implements AutoCloseable {

  private static final String CLOUD_IDS_PATH = "/cloudIds";
  private static final String P_CLOUD_ID = "CLOUD_ID";
  private static final String P_PROVIDER_ID = "PROVIDER_ID";
  private static final String DATA_PROVIDERS_PATH_WITH_PROVIDER_ID = "/data-providers/{PROVIDER_ID}";
  private static final String DATA_PROVIDERS_PATH = "/data-providers";
  private static final String CLOUD_IDS_PATH_WITH_CLOUD_ID = "/cloudIds/{CLOUD_ID}";

  private static final String OTHER_CLIENT_MESSAGE = "Other client error";

  private static final Logger LOGGER = LoggerFactory.getLogger(UISClient.class);
  private static final int DEFAULT_CONNECT_TIMEOUT_IN_MILLIS = 20000;
  private static final int DEFAULT_READ_TIMEOUT_IN_MILLIS = 60000;
  private UrlProvider urlProvider;

  protected final Client client =
      ClientBuilder
          .newBuilder()
          .register(JacksonFeature.class)
          .build();


  /**
   * Creates a new instance of this class. Same as {@link #UISClient(String)} but includes username and password to perform
   * authenticated requests.
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
   * @param recordId The record Id
   * @return The newly generated CloudId
   * @throws CloudException The generic cloud exception wrapper
   */
  public CloudId createCloudId(String providerId, String recordId) throws CloudException {
    return manageResponse(new ResponseParams<>(CloudId.class), () -> client
        .target(urlProvider.getBaseUrl())
        .path(CLOUD_IDS_PATH)
        .queryParam(UISParamConstants.Q_PROVIDER_ID, providerId)
        .queryParam(UISParamConstants.Q_RECORD_ID, recordId).request()
        .post(null)
    );
  }

  /**
   * Invoke the creation of a new CloudId REST call.
   *
   * @param providerId The provider Id
   * @return The newly generated CloudId
   * @throws CloudException The generic cloud exception wrapper
   */
  public CloudId createCloudId(String providerId) throws CloudException {
    return manageResponse(new ResponseParams<>(CloudId.class), () -> client
        .target(urlProvider.getBaseUrl())
        .path(CLOUD_IDS_PATH)
        .queryParam(UISParamConstants.Q_PROVIDER_ID, providerId)
        .request()
        .post(null)
    );
  }


  /**
   * Invoke the retrieval of a cloud identifier.
   *
   * @param providerId The provider Id
   * @param recordId The record Id
   * @return The retrieved cloud Id
   * @throws CloudException The generic cloud exception wrapper
   */
  public CloudId getCloudId(String providerId, String recordId) throws CloudException {
    return manageResponse(new ResponseParams<>(CloudId.class), () -> client
        .target(urlProvider.getBaseUrl())
        .path(CLOUD_IDS_PATH)
        .queryParam(UISParamConstants.Q_PROVIDER_ID, providerId)
        .queryParam(UISParamConstants.Q_RECORD_ID, recordId)
        .request()
        .get()
    );
  }

  /**
   * Retrieve the local identifiers associated with a cloud identifier
   *
   * @param cloudId The cloud id to search for
   * @return The List of local ids associated with the cloud id
   * @throws CloudException The generic cloud exception wrapper
   */
  @SuppressWarnings("unchecked")
  public ResultSlice<CloudId> getRecordId(String cloudId) throws CloudException {
    return manageResponse(new ResponseParams<>(ResultSlice.class), () -> client
        .target(urlProvider.getBaseUrl())
        .path(CLOUD_IDS_PATH_WITH_CLOUD_ID)
        .resolveTemplate(P_CLOUD_ID, cloudId)
        .request()
        .get()
    );
  }


  /**
   * Retrieve records associated with a provider.
   *
   * @param providerId The provider Id
   * @return The List of Local ids associated with a provider
   * @throws CloudException The generic cloud exception wrapper
   */
  @SuppressWarnings("unchecked")
  public ResultSlice<LocalId> getRecordIdsByProvider(String providerId) throws CloudException {
    return manageResponse(new ResponseParams<>(ResultSlice.class), () -> client
        .target(urlProvider.getBaseUrl())
        .path("/data-providers/{PROVIDER_ID}/localIds")
        .resolveTemplate(P_PROVIDER_ID, providerId)
        .request()
        .get()
    );
  }


  /**
   * Retrieve the Cloud ids associated with a provider.
   *
   * @param providerId The provider id
   * @return The list of cloud ids associated with the provider id
   * @throws CloudException The generic cloud exception wrapper
   */
  @SuppressWarnings("unchecked")
  public ResultSlice<CloudId> getCloudIdsByProvider(String providerId) throws CloudException {
    return manageResponse(new ResponseParams<>(ResultSlice.class), () -> client
        .target(urlProvider.getBaseUrl())
        .path("/data-providers/{PROVIDER_ID}/cloudIds")
        .resolveTemplate(P_PROVIDER_ID, providerId)
        .request()
        .get()
    );
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
  public ResultSlice<LocalId> getRecordIdsByProviderWithPagination(String providerId, String startRecordId, int limit)
      throws CloudException {
    return manageResponse(new ResponseParams<>(ResultSlice.class), () -> client
        .target(urlProvider.getBaseUrl())
        .path("/data-providers/{PROVIDER_ID}/localIds")
        .resolveTemplate(P_PROVIDER_ID, providerId)
        .queryParam(UISParamConstants.Q_FROM, startRecordId)
        .queryParam(UISParamConstants.Q_LIMIT, limit)
        .request()
        .get()
    );
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
    return manageResponse(new ResponseParams<>(ResultSlice.class), () -> client
        .target(urlProvider.getBaseUrl()).path("/data-providers/{PROVIDER_ID}/cloudIds")
        .resolveTemplate(P_PROVIDER_ID, providerId)
        .queryParam(UISParamConstants.Q_FROM, startRecordId)
        .queryParam(UISParamConstants.Q_LIMIT, limit).request().get()
    );
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
  public boolean createMapping(String cloudId, String providerId, String recordId) throws CloudException {
    return manageResponse(new ResponseParams<>(Boolean.class), () -> client
        .target(urlProvider.getBaseUrl()).path("/data-providers/{PROVIDER_ID}/cloudIds/{CLOUD_ID}")
        .resolveTemplate(P_PROVIDER_ID, providerId)
        .resolveTemplate(P_CLOUD_ID, cloudId)
        .queryParam(UISParamConstants.Q_RECORD_ID, recordId).request()
        .post(null)
    );
  }

  /**
   * Create a data provider.
   *
   * @param providerId The data provider Id
   * @param dp The data provider properties
   * @return A URL that points to the data provider
   * @throws CloudException throws common {@link CloudException} if something went wrong
   */
  public String createProvider(String providerId, DataProviderProperties dp) throws CloudException {
    return manageResponse(new ResponseParams<>(String.class, Response.Status.CREATED), () -> client
        .target(urlProvider.getBaseUrl()).path(DATA_PROVIDERS_PATH)
        .queryParam(UISParamConstants.Q_PROVIDER, providerId)
        .request()
        .post(Entity.json(dp))
    );
  }


  /**
   * Update a Data Provider.
   *
   * @param providerId The provider to update
   * @param dp The data provider properties
   * @return True if successful, false else
   * @throws CloudException throws common {@link CloudException} if something went wrong
   */
  public boolean updateProvider(String providerId, DataProviderProperties dp) throws CloudException {
    return manageResponse(new ResponseParams<>(Boolean.class, Response.Status.NO_CONTENT), () -> client
        .target(urlProvider.getBaseUrl())
        .path(DATA_PROVIDERS_PATH_WITH_PROVIDER_ID)
        .resolveTemplate(P_PROVIDER_ID, providerId)
        .request()
        .put(Entity.json(dp))
    );
  }


  /**
   * Get data providers
   *
   * @param from The record to start from
   * @return A predefined number of data providers
   * @throws CloudException throws common {@link CloudException} if something went wrong
   */
  @SuppressWarnings("unchecked")
  public ResultSlice<DataProvider> getDataProviders(String from) throws CloudException {
    return manageResponse(new ResponseParams<>(ResultSlice.class), () -> client
        .target(urlProvider.getBaseUrl())
        .path(DATA_PROVIDERS_PATH)
        .queryParam(UISParamConstants.Q_FROM, from)
        .request()
        .get()
    );
  }

  /**
   * Retrieve a selected data provider.
   *
   * @param providerId The provider id to retrieve
   * @return The Data provider that corresponds to the selected id
   * @throws CloudException throws common {@link CloudException} if something went wrong
   */
  public DataProvider getDataProvider(String providerId) throws CloudException {
    return manageResponse(new ResponseParams<>(DataProvider.class), () -> client
        .target(urlProvider.getBaseUrl())
        .path(DATA_PROVIDERS_PATH_WITH_PROVIDER_ID)
        .resolveTemplate(P_PROVIDER_ID, providerId)
        .request()
        .get()
    );
  }

  @SuppressWarnings("unchecked")
  protected <T> T manageResponse(ResponseParams<T> responseParameters, Supplier<Response> responseSupplier)
      throws CloudException {
    Response response = responseSupplier.get();
    try {
      response.bufferEntity();
      if (response.getStatus() == responseParameters.getValidStatus().getStatusCode()) {
        return (responseParameters.getExpectedClass() == Boolean.class) ? (T) Boolean.TRUE
            : response.readEntity(responseParameters.getExpectedClass());
      }
      ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
      IdentifierErrorTemplate error = IdentifierErrorTemplate.valueOf(errorInfo.getErrorCode());

      throw createException(errorInfo.getDetails(), errorInfo.getErrorCode(), error.getException(errorInfo));
    } catch (CloudException cloudException) {
      throw cloudException; //re-throw just created CloudException
    } catch (ProcessingException processingException) {
      String message = String.format("Could not deserialize response with statusCode: %d; message: %s",
          response.getStatus(), response.readEntity(String.class));
      throw createException(message, message, processingException);
    } catch (Exception otherExceptions) {
      throw createException(OTHER_CLIENT_MESSAGE, OTHER_CLIENT_MESSAGE, otherExceptions);
    } finally {
      closeResponse(response);
    }
  }


  public void close() {
    client.close();
  }

  /**
   * Creates the exception to be returned to the client.
   *
   * @param errorMessage - Messages to show in log file
   * @param errorCode - Message used in #CloudException as a message (first parameter)
   * @param causeException - Local (client-side) exception for log output. Can be null
   * @return A CloudException that wraps the original exception
   */
  private CloudException createException(String errorMessage, String errorCode, Exception causeException) {
    LOGGER.error(errorMessage, causeException);
    return new CloudException(errorCode, causeException);
  }

  private void closeResponse(Response response) {
    if (response != null) {
      response.close();
    }
  }

  @Override
  @Deprecated(forRemoval = true)
  protected void finalize() throws Throwable {
    LOGGER.warn("'{}.finalize()' called!!!\n{}", getClass().getSimpleName(), Thread.currentThread().getStackTrace());
    client.close();
  }


  @Getter
  protected static class ResponseParams<T> {

    private final Class<T> expectedClass;
    private final Response.Status validStatus;

    public ResponseParams(Class<T> expectedClass) {
      this(expectedClass, Response.Status.OK);
    }

    private ResponseParams(Class<T> expectedClass, Response.Status validStatus) {
      this.expectedClass = expectedClass;
      this.validStatus = validStatus;
    }
  }

}
