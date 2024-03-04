package eu.europeana.cloud.client.dps.rest;

import static eu.europeana.cloud.service.dps.RestInterfaceConstants.DATASET_ID;
import static eu.europeana.cloud.service.dps.RestInterfaceConstants.DETAILED_TASK_REPORT_URL;
import static eu.europeana.cloud.service.dps.RestInterfaceConstants.ELEMENT_REPORT_URL;
import static eu.europeana.cloud.service.dps.RestInterfaceConstants.ERROR;
import static eu.europeana.cloud.service.dps.RestInterfaceConstants.ERRORS_TASK_REPORT_URL;
import static eu.europeana.cloud.service.dps.RestInterfaceConstants.IDS_COUNT;
import static eu.europeana.cloud.service.dps.RestInterfaceConstants.KILL_TASK_URL;
import static eu.europeana.cloud.service.dps.RestInterfaceConstants.METIS_DATASET_PUBLISHED_RECORDS_SEARCH;
import static eu.europeana.cloud.service.dps.RestInterfaceConstants.PERMIT_TOPOLOGY_URL;
import static eu.europeana.cloud.service.dps.RestInterfaceConstants.STATISTICS_REPORT_URL;
import static eu.europeana.cloud.service.dps.RestInterfaceConstants.TASKS_URL;
import static eu.europeana.cloud.service.dps.RestInterfaceConstants.TASK_CLEAN_DATASET_URL;
import static eu.europeana.cloud.service.dps.RestInterfaceConstants.TASK_ID;
import static eu.europeana.cloud.service.dps.RestInterfaceConstants.TASK_PROGRESS_URL;
import static eu.europeana.cloud.service.dps.RestInterfaceConstants.TOPOLOGY_NAME;

import eu.europeana.cloud.common.model.dps.MetisDataset;
import eu.europeana.cloud.common.model.dps.NodeReport;
import eu.europeana.cloud.common.model.dps.StatisticsReport;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.common.model.dps.TaskErrorsInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.RestInterfaceConstants;
import eu.europeana.cloud.service.dps.exception.DPSClientException;
import eu.europeana.cloud.service.dps.exception.DPSExceptionProvider;
import eu.europeana.cloud.service.dps.exception.DpsException;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.ServiceUnavailableException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Form;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The REST API client for the Data Processing service.
 */
public class DpsClient implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(DpsClient.class);

  private static final int DEFAULT_CONNECT_TIMEOUT_IN_MILLIS = 20000;
  private static final int DEFAULT_READ_TIMEOUT_IN_MILLIS = 60000;
  public static final String TASK_CANT_BE_KILLED_MESSAGE = "Task Can't be killed";
  private final String dpsUrl;

  private final Client client =
      ClientBuilder.newBuilder()
                   .register(JacksonFeature.class)
                   .build();

  /**
   * Creates a new instance of this class.
   *
   * @param dpsUrl Url where the DPS service is located.
   * @param username THe username to perform authenticated requests.
   * @param password THe username to perform authenticated requests.
   * @param connectTimeoutInMillis Connection timeout in milliseconds (timeout for establishing the remote connection).
   * @param readTimeoutInMillis The read timeout in milliseconds (timeout for obtaining/1reading the result).
   */
  public DpsClient(final String dpsUrl, final String username, final String password,
      final int connectTimeoutInMillis, final int readTimeoutInMillis) {
    this.client.register(HttpAuthenticationFeature.basic(username, password));
    this.client.property(ClientProperties.CONNECT_TIMEOUT, connectTimeoutInMillis);
    this.client.property(ClientProperties.READ_TIMEOUT, readTimeoutInMillis);
    this.dpsUrl = dpsUrl;
  }

  /**
   * Creates a new instance of this class. Will use a default connect timeout of {@value #DEFAULT_CONNECT_TIMEOUT_IN_MILLIS} and a
   * default read timeout of {@link #DEFAULT_READ_TIMEOUT_IN_MILLIS}.
   *
   * @param dpsUrl Url where the DPS service is located.
   * @param username THe username to perform authenticated requests.
   * @param password THe username to perform authenticated requests.
   */
  public DpsClient(final String dpsUrl, final String username, final String password) {
    this(dpsUrl, username, password, DEFAULT_CONNECT_TIMEOUT_IN_MILLIS, DEFAULT_READ_TIMEOUT_IN_MILLIS);
  }


  /**
   * Creates a new instance of this class.
   *
   * @param dpsUrl Url where the DPS service is located.
   * @param connectTimeoutInMillis Connection timeout in milliseconds (timeout for establishing the remote connection).
   * @param readTimeoutInMillis The read timeout in milliseconds (timeout for obtaining/1reading the result).
   */
  public DpsClient(final String dpsUrl, final int connectTimeoutInMillis,
      final int readTimeoutInMillis) {
    this.client.property(ClientProperties.CONNECT_TIMEOUT, connectTimeoutInMillis);
    this.client.property(ClientProperties.READ_TIMEOUT, readTimeoutInMillis);
    this.dpsUrl = dpsUrl;
  }


  /**
   * Creates a new instance of this class. Will use a default connect timeout of {@value #DEFAULT_CONNECT_TIMEOUT_IN_MILLIS} and a
   * default read timeout of {@link #DEFAULT_READ_TIMEOUT_IN_MILLIS}.
   *
   * @param dpsUrl Url where the DPS service is located.
   */
  public DpsClient(final String dpsUrl) {
    this(dpsUrl, DEFAULT_CONNECT_TIMEOUT_IN_MILLIS, DEFAULT_READ_TIMEOUT_IN_MILLIS);
  }

  /**
   * Submits a task for execution in the specified topology.
   */
  public long submitTask(DpsTask task, String topologyName) throws DpsException {
    URI uri = manageResponse(
        new ResponseParams<>(URI.class, Response.Status.CREATED),
        () -> client.target(dpsUrl)
                    .path(TASKS_URL)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName)
                    .request()
                    .post(Entity.json(task)), "Submit Task Was not successful"
    );

    return getTaskId(uri);
  }

  /**
   * clean METIS indexing dataset.
   */
  public void cleanMetisIndexingDataset(String topologyName, long taskId,
      DataSetCleanerParameters dataSetCleanerParameters) throws DpsException {
    manageResponse(new ResponseParams<>(Void.class),
        () -> client.target(dpsUrl)
                    .path(TASK_CLEAN_DATASET_URL)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName)
                    .resolveTemplate(TASK_ID, taskId)
                    .request()
                    .post(Entity.json(dataSetCleanerParameters)),
        "Cleaning a dataset was not successful");
  }

  /**
   * permit user to use topology
   */
  public Response.StatusType topologyPermit(String topologyName, String username) throws DpsException {
    Form form = new Form();
    form.param("username", username);

    return manageResponse(new ResponseParams<>(Response.StatusType.class),
        () -> client.target(dpsUrl)
                    .path(PERMIT_TOPOLOGY_URL)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName)
                    .request()
                    .post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE)),
        "Granting permission was not successful");
  }

  /**
   * Retrieves progress for the specified combination of taskId and topology.
   */
  public TaskInfo getTaskProgress(String topologyName, final long taskId) throws DpsException {
    return manageResponse(new ResponseParams<>(TaskInfo.class),
        () -> client
            .target(dpsUrl)
            .path(TASK_PROGRESS_URL)
            .resolveTemplate(TOPOLOGY_NAME, topologyName)
            .resolveTemplate(TASK_ID, taskId)
            .request()
            .get(),
        "Task progress cannot be read");
  }

  /**
   * Retrieves number of elements in the Metis dataset
   *
   * @param datasetId dataset identifier
   * @param database database that will be used as source of true. Allowed values are PUBLISH and PREVIEW
   * {@link TargetIndexingDatabase}
   * @return number of elements in the dataset
   * @throws DpsException throws common {@link DpsException} if something went wrong
   */
  public long getTotalMetisDatabaseRecords(String datasetId, TargetIndexingDatabase database) throws DpsException {
    MetisDataset metisDataset = manageResponse(new ResponseParams<>(MetisDataset.class),
        () -> client
            .target(dpsUrl)
            .path(RestInterfaceConstants.METIS_DATASETS)
            .resolveTemplate("datasetId", datasetId)
            .queryParam("database", database)
            .request()
            .get(),
        "Error while retrieving total metis database records");
    return metisDataset.getSize();
  }

  public List<SubTaskInfo> getDetailedTaskReport(final String topologyName, final long taskId) throws DpsException {
    return manageResponse(new ResponseParams<>(new GenericType<List<SubTaskInfo>>() {
        }),
        () -> client
            .target(dpsUrl)
            .path(DETAILED_TASK_REPORT_URL)
            .resolveTemplate(TOPOLOGY_NAME, topologyName)
            .resolveTemplate(TASK_ID, taskId)
            .request()
            .get(),
        "Error while retrieving detailed task report"
    );
  }

  public List<SubTaskInfo> getDetailedTaskReportBetweenChunks(
      final String topologyName, final long taskId, int from, int to) throws DpsException {
    //
    return manageResponse(
        new ResponseParams<>(new GenericType<List<SubTaskInfo>>() {
        }),
        () -> client
            .target(dpsUrl)
            .path(DETAILED_TASK_REPORT_URL)
            .resolveTemplate(TOPOLOGY_NAME, topologyName)
            .resolveTemplate(TASK_ID, taskId)
            .queryParam("from", from)
            .queryParam("to", to)
            .request()
            .get(),
        "Error while retrieving detailed task report (witch chunks)");
  }

  @SuppressWarnings("all")
  public List<NodeReport> getElementReport(final String topologyName, final long taskId, String elementPath) throws DpsException {
    return manageResponse(new ResponseParams<>(new GenericType<List<NodeReport>>() {
        }),
        () -> client
            .target(dpsUrl)
            .path(ELEMENT_REPORT_URL)
            .resolveTemplate(TOPOLOGY_NAME, topologyName)
            .resolveTemplate(TASK_ID, taskId)
            .queryParam("path", elementPath)
            .request().get(),
        "Error while retrieving reports element");
  }

  public TaskErrorsInfo getTaskErrorsReport(final String topologyName, final long taskId,
      final String error, final int idsCount) throws DpsException {
    return manageResponse(new ResponseParams<>(TaskErrorsInfo.class),
        () -> client
            .target(dpsUrl)
            .path(ERRORS_TASK_REPORT_URL)
            .resolveTemplate(TOPOLOGY_NAME, topologyName)
            .resolveTemplate(TASK_ID, taskId)
            .queryParam(ERROR, error)
            .queryParam(IDS_COUNT, idsCount)
            .request().get(),
        "Error while retrieving task error report");
  }

  public boolean checkIfErrorReportExists(final String topologyName, final long taskId) throws DpsException {
    return manageResponse(
        new ResponseParams<>(Response.StatusType.class, Response.Status.OK, Response.Status.METHOD_NOT_ALLOWED),
        () -> client
            .target(dpsUrl)
            .path(ERRORS_TASK_REPORT_URL)
            .resolveTemplate(TOPOLOGY_NAME, topologyName)
            .resolveTemplate(TASK_ID, taskId)
            .request().head(),
        "Error while checking error report exits")
        .getStatusCode() == Response.Status.OK.getStatusCode();
  }

  public StatisticsReport getTaskStatisticsReport(final String topologyName, final long taskId) throws DpsException {
    return manageResponse(new ResponseParams<>(StatisticsReport.class),
        () -> client.target(dpsUrl)
                    .path(STATISTICS_REPORT_URL)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName)
                    .resolveTemplate(TASK_ID, taskId)
                    .request()
                    .get(),
        "Task statistics report cannot be read"
    );
  }

  public String killTask(final String topologyName, final long taskId, String info) throws DpsException {
    if (info == null) {
      return killTask(topologyName, taskId);
    }
    return manageResponse(new ResponseParams<>(String.class),
        () -> client
            .target(dpsUrl)
            .path(KILL_TASK_URL)
            .resolveTemplate(TOPOLOGY_NAME, topologyName)
            .resolveTemplate(TASK_ID, taskId)
            .queryParam("info", info)
            .request()
            .post(null),
        TASK_CANT_BE_KILLED_MESSAGE);
  }

  private String killTask(final String topologyName, final long taskId) throws DpsException {
    return manageResponse(new ResponseParams<>(String.class),
        () -> client
            .target(dpsUrl)
            .path(KILL_TASK_URL)
            .resolveTemplate(TOPOLOGY_NAME, topologyName)
            .resolveTemplate(TASK_ID, taskId)
            .request()
            .post(null),
        TASK_CANT_BE_KILLED_MESSAGE);
  }

  public List<String> searchPublishedDatasetRecords(final String datasetId, final List<String> recordsIds) throws DpsException {
    return manageResponse(new ResponseParams<>(new GenericType<List<String>>() {
        }),
        () -> client.target(dpsUrl)
                    .path(METIS_DATASET_PUBLISHED_RECORDS_SEARCH)
                    .resolveTemplate(DATASET_ID, datasetId)
                    .request()
                    .post(Entity.json(recordsIds)),
        "Error while searching published dataset records"
    );
  }

  @Override
  public void close() {
    client.close();
  }

  private long getTaskId(URI uri) {
    String[] elements = uri.getRawPath().split("/");
    return Long.parseLong(elements[elements.length - 1]);
  }

  private <T> T manageResponse(ResponseParams<T> responseParameters, Supplier<Response> responseSupplier, String errorMessage)
      throws DpsException {
    Response response = responseSupplier.get();
    try {
      response.bufferEntity();
      if (responseParameters.isStatusCodeValid(response.getStatus())) {
        return readEntityByClass(responseParameters, response);
      } else if (response.getStatus() == HttpURLConnection.HTTP_UNAVAILABLE) {
        throw DPSExceptionProvider.createException(errorMessage, "Service unavailable", new ServiceUnavailableException());
      } else if (response.getStatus() == HttpURLConnection.HTTP_NOT_FOUND) {
        throw DPSExceptionProvider.createException(errorMessage, "Endpoint not found", new NotFoundException());
      }
      ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
      throw DPSExceptionProvider.createException(errorInfo);
    } catch (DpsException | DPSClientException dpsException) {
      throw dpsException; //re-throw just created DpsException
    } catch (ProcessingException processingException) {
      String message = String.format(
          "Could not deserialize response with statusCode: %d; message: %s", response.getStatus(),
          response.readEntity(String.class));
      throw DPSExceptionProvider.createException(errorMessage, message, processingException);
    } catch (Exception otherExceptions) {
      throw DPSExceptionProvider.createException(errorMessage, "Other exception", otherExceptions);
    } finally {
      closeResponse(response);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T readEntityByClass(ResponseParams<T> responseParameters, Response response) {
    if (responseParameters.getExpectedClass() == Void.class) {
      return null;
    } else if (responseParameters.getExpectedClass() == Boolean.class) {
      return (T) Boolean.TRUE;
    } else if (responseParameters.getExpectedClass() == URI.class) {
      return (T) response.getLocation();
    } else if (responseParameters.getExpectedClass() == Response.StatusType.class) {
      return (T) response.getStatusInfo();
    } else if (responseParameters.getGenericType() != null) {
      return response.readEntity(responseParameters.getGenericType());
    } else {
      return response.readEntity(responseParameters.getExpectedClass());
    }
  }

  private void closeResponse(Response response) {
    if (response != null) {
      response.close();
    }
  }

  @Override
  protected void finalize() throws Throwable {
    LOGGER.warn("'{}.finalize()' called!!!\n{}", getClass().getSimpleName(), Thread.currentThread().getStackTrace());
    client.close();
  }

  @AllArgsConstructor
  @Getter
  private static class ResponseParams<T> {

    private Class<T> expectedClass;
    private GenericType<T> genericType;
    private Response.Status[] validStatuses;

    public ResponseParams(Class<T> expectedClass) {
      this(expectedClass, Response.Status.OK);
    }

    public ResponseParams(Class<T> expectedClass, Response.Status validStatus) {
      this(expectedClass, null, new Response.Status[]{validStatus});
    }

    public ResponseParams(GenericType<T> genericType) {
      this(null, genericType, new Response.Status[]{Response.Status.OK});
    }

    public ResponseParams(Class<T> expectedClass, Response.Status... validStatuses) {
      this(expectedClass, null, validStatuses);
    }

    public boolean isStatusCodeValid(Integer statusCode) {
      return Arrays.stream(validStatuses)
                   .map(Response.Status::getStatusCode)
                   .anyMatch(statusCode::equals);
    }
  }

}
