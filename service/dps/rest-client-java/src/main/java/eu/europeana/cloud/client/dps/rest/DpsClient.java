package eu.europeana.cloud.client.dps.rest;

import eu.europeana.cloud.common.model.dps.*;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.exception.DPSExceptionProvider;
import eu.europeana.cloud.service.dps.exception.DpsException;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.List;

/**
 * The REST API client for the Data Processing service.
 */
public class DpsClient {

    private static final String ERROR = "error";
    private static final String IDS_COUNT = "idsCount";
    private Logger LOGGER = LoggerFactory.getLogger(DpsClient.class);

    private String dpsUrl;

    private final Client client = JerseyClientBuilder.newClient();

    private static final String TOPOLOGY_NAME = "TopologyName";
    private static final String TASK_ID = "TaskId";
    private static final String TASKS_URL = "/{" + TOPOLOGY_NAME + "}/tasks";
    private static final String PERMIT_TOPOLOGY_URL = "/{" + TOPOLOGY_NAME + "}/permit";
    private static final String TASK_URL = TASKS_URL + "/{" + TASK_ID + "}";
    private static final String REPORTS_RESOURCE = "reports";
    private static final String STATISTICS_RESOURCE = "statistics";
    private static final String KILL_TASK = "kill";

    private static final String TASK_PROGRESS_URL = TASK_URL + "/progress";
    private static final String TASK_CLEAN_DATASET_URL = TASK_URL + "/cleaner";
    private static final String DETAILED_TASK_REPORT_URL = TASK_URL + "/" + REPORTS_RESOURCE + "/details";
    private static final String ERRORS_TASK_REPORT_URL = TASK_URL + "/" + REPORTS_RESOURCE + "/errors";
    private static final String STATISTICS_REPORT_URL = TASK_URL + "/" + STATISTICS_RESOURCE;
    private static final String KILL_TASK_URL = TASK_URL + "/" + KILL_TASK;
    private static final String ELEMENT_REPORT = TASK_URL + "/" + REPORTS_RESOURCE + "/element";
    private static final int DEFAULT_CONNECT_TIMEOUT_IN_MILLIS = 20000;
    private static final int DEFAULT_READ_TIMEOUT_IN_MILLIS = 60000;

    /**
     * Creates a new instance of this class.
     *
     * @param dpsUrl                 Url where the DPS service is located.
     * @param username               THe username to perform authenticated requests.
     * @param password               THe username to perform authenticated requests.
     * @param connectTimeoutInMillis The connect timeout in milliseconds (timeout for establishing the
     *                               remote connection).
     * @param readTimeoutInMillis    The read timeout in milliseconds (timeout for obtaining/1reading
     *                               the result).
     */
    public DpsClient(final String dpsUrl, final String username, final String password,
                     final int connectTimeoutInMillis, final int readTimeoutInMillis) {
        this.client.register(HttpAuthenticationFeature.basic(username, password));
        this.client.property(ClientProperties.CONNECT_TIMEOUT, connectTimeoutInMillis);
        this.client.property(ClientProperties.READ_TIMEOUT, readTimeoutInMillis);
        this.dpsUrl = dpsUrl;
    }

    /**
     * Creates a new instance of this class. Will use a default connect timeout of
     * {@value #DEFAULT_CONNECT_TIMEOUT_IN_MILLIS} and a default read timeout of
     * {@link #DEFAULT_READ_TIMEOUT_IN_MILLIS}.
     *
     * @param dpsUrl   Url where the DPS service is located.
     * @param username THe username to perform authenticated requests.
     * @param password THe username to perform authenticated requests.
     */
    public DpsClient(final String dpsUrl, final String username, final String password) {
        this(dpsUrl, username, password, DEFAULT_CONNECT_TIMEOUT_IN_MILLIS, DEFAULT_READ_TIMEOUT_IN_MILLIS);
    }


    /**
     * Creates a new instance of this class.
     *
     * @param dpsUrl                 Url where the DPS service is located.
     * @param connectTimeoutInMillis The connect timeout in milliseconds (timeout for establishing the
     *                               remote connection).
     * @param readTimeoutInMillis    The read timeout in milliseconds (timeout for obtaining/1reading
     *                               the result).
     */
    public DpsClient(final String dpsUrl, final int connectTimeoutInMillis, final int readTimeoutInMillis) {
        this.client.property(ClientProperties.CONNECT_TIMEOUT, connectTimeoutInMillis);
        this.client.property(ClientProperties.READ_TIMEOUT, readTimeoutInMillis);
        this.dpsUrl = dpsUrl;
    }


    /**
     * Creates a new instance of this class. Will use a default connect timeout of
     * {@value #DEFAULT_CONNECT_TIMEOUT_IN_MILLIS} and a default read timeout of
     * {@link #DEFAULT_READ_TIMEOUT_IN_MILLIS}.
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

        Response resp = null;
        try {
            resp = client.target(dpsUrl)
                    .path(TASKS_URL)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName)
                    .request()
                    .post(Entity.json(task));

            if (resp.getStatus() == Response.Status.CREATED.getStatusCode())
                return getTaskId(resp.getLocation());
            else {
                LOGGER.error("Submit Task Was not successful");
                throw handleException(resp);
            }
        } finally {
            closeResponse(resp);
        }
    }


    /**
     * clean METIS indexing dataset.
     */
    public void cleanMetisIndexingDataset(String topologyName, long taskId, DataSetCleanerParameters dataSetCleanerParameters) throws DpsException {

        Response resp = null;
        try {
            resp = client.target(dpsUrl)
                    .path(TASK_CLEAN_DATASET_URL)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName)
                    .resolveTemplate(TASK_ID, taskId)
                    .request()
                    .post(Entity.json(dataSetCleanerParameters));

            if (resp.getStatus() != Response.Status.OK.getStatusCode()) {
                LOGGER.error("Cleaning a dataset was not successful");
                throw handleException(resp);
            }
        } finally {
            closeResponse(resp);
        }

    }


    /**
     * clean METIS indexing dataset.
     */
    public void cleanMetisIndexingDataset(String topologyName, long taskId, DataSetCleanerParameters dataSetCleanerParameters, String key, String value) throws DpsException {

        Response resp = null;
        try {
            resp = client.target(dpsUrl)
                    .path(TASK_CLEAN_DATASET_URL)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName)
                    .resolveTemplate(TASK_ID, taskId)
                    .request().header(key, value)
                    .post(Entity.json(dataSetCleanerParameters));

            if (resp.getStatus() != Response.Status.OK.getStatusCode()) {
                LOGGER.error("Cleaning a dataset was not successful");
                throw handleException(resp);
            }
        } finally {
            closeResponse(resp);
        }

    }


    private long getTaskId(URI uri) {
        String[] elements = uri.getRawPath().split("/");
        return Long.parseLong(elements[elements.length - 1]);
    }

    /**
     * permit user to use topology
     */
    public Response.StatusType topologyPermit(String topologyName, String username) throws DpsException {
        Form form = new Form();
        form.param("username", username);
        Response resp = null;

        try {
            resp = client.target(dpsUrl)
                    .path(PERMIT_TOPOLOGY_URL)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName)
                    .request()
                    .post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));

            if (resp.getStatus() == Response.Status.OK.getStatusCode())
                return resp.getStatusInfo();
            else {
                LOGGER.error("Granting permission was not successful");
                throw handleException(resp);
            }
        } finally {
            closeResponse(resp);
        }
    }


    /**
     * Retrieves progress for the specified combination of taskId and topology.
     */
    public TaskInfo getTaskProgress(String topologyName, final long taskId) throws DpsException {

        Response response = null;

        try {
            response = client
                    .target(dpsUrl)
                    .path(TASK_PROGRESS_URL)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName)
                    .resolveTemplate(TASK_ID, taskId)
                    .request().get();

            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                return response.readEntity(TaskInfo.class);
            } else {
                LOGGER.error("Task progress cannot be read");
                throw handleException(response);
            }
        } finally {
            closeResponse(response);
        }
    }

    public List<SubTaskInfo> getDetailedTaskReport(final String topologyName, final long taskId) throws DpsException {

        Response response = null;

        try {
            response = client
                    .target(dpsUrl)
                    .path(DETAILED_TASK_REPORT_URL)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName)
                    .resolveTemplate(TASK_ID, taskId)
                    .request().get();

            return handleResponse(response);

        } finally {
            closeResponse(response);
        }
    }

    public List<SubTaskInfo> getDetailedTaskReportBetweenChunks(final String topologyName, final long taskId, int from, int to) throws DpsException {

        Response getResponse = null;

        try {
            getResponse = client
                    .target(dpsUrl)
                    .path(DETAILED_TASK_REPORT_URL)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName)
                    .resolveTemplate(TASK_ID, taskId).queryParam("from", from).queryParam("to", to)
                    .request().get();

            return handleResponse(getResponse);

        } finally {
            closeResponse(getResponse);
        }
    }


    public List<NodeReport> getElementReport(final String topologyName, final long taskId, String elementPath) throws DpsException {

        Response getResponse = null;

        try {
            getResponse = client
                    .target(dpsUrl)
                    .path(ELEMENT_REPORT)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName)
                    .resolveTemplate(TASK_ID, taskId).queryParam("path", elementPath)
                    .request().get();

            return handleElementReportResponse(getResponse);

        } finally {
            closeResponse(getResponse);
        }
    }


    private List<NodeReport> handleElementReportResponse(Response response) throws DpsException {
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(new GenericType<List<NodeReport>>() {
            });
        } else {
            throw handleException(response);
        }
    }


    private List<SubTaskInfo> handleResponse(Response response) throws DpsException {
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(new GenericType<List<SubTaskInfo>>() {
            });
        } else {
            throw handleException(response);
        }
    }

    private void closeResponse(Response response) {
        if (response != null) {
            response.close();
        }
    }

    public TaskErrorsInfo getTaskErrorsReport(final String topologyName, final long taskId, final String error, final int idsCount) throws DpsException {

        Response response = null;

        try {
            response = client
                    .target(dpsUrl)
                    .path(ERRORS_TASK_REPORT_URL)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName)
                    .resolveTemplate(TASK_ID, taskId)
                    .queryParam(ERROR, error)
                    .queryParam(IDS_COUNT, idsCount)
                    .request().get();

            return handleErrorResponse(response);

        } finally {
            closeResponse(response);
        }
    }

    public boolean checkIfErrorReportExists(final String topologyName, final long taskId) {

        Response response = null;

        try {
            response = client
                    .target(dpsUrl)
                    .path(ERRORS_TASK_REPORT_URL)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName)
                    .resolveTemplate(TASK_ID, taskId)
                    .request().head();

            if (response.getStatus() == Response.Status.OK.getStatusCode())
                return true;
            return false;

        } finally {
            closeResponse(response);
        }
    }


    private TaskErrorsInfo handleErrorResponse(Response response) throws DpsException {
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(TaskErrorsInfo.class);
        } else {
            LOGGER.error("Task error report cannot be read");
            throw handleException(response);
        }
    }

    public StatisticsReport getTaskStatisticsReport(final String topologyName, final long taskId) throws DpsException {
        Response response = null;
        try {
            response = client.target(dpsUrl).path(STATISTICS_REPORT_URL)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName).resolveTemplate(TASK_ID, taskId).request().get();

            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                return response.readEntity(StatisticsReport.class);
            } else {
                LOGGER.error("Task statistics report cannot be read");
                throw handleException(response);
            }
        } finally {
            closeResponse(response);
        }
    }

    public String killTask(final String topologyName, final long taskId, String info) throws DpsException {
        Response response = null;
        try {
            WebTarget webTarget = client.target(dpsUrl).path(KILL_TASK_URL)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName).resolveTemplate(TASK_ID, taskId);
            if (info != null)
                webTarget = webTarget.queryParam("info", info);
            response = webTarget.request().post(null);
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                return response.readEntity(String.class);
            } else {
                LOGGER.error("Task Can't be killed");
                throw handleException(response);
            }
        } finally {
            closeResponse(response);
        }
    }

    private DpsException handleException(Response response) throws DpsException {
        try {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            return DPSExceptionProvider.generateException(errorInfo);
        } catch (Exception e) {
            return new DpsException("Unexpected Exception happened while communicating with DPS, Check your request!");
        }

    }

    @Override
    protected void finalize() throws Throwable {
        client.close();
    }

    public void close() {
        client.close();
    }
}



