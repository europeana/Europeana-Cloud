package eu.europeana.cloud.client.dps.rest;

import eu.europeana.cloud.common.model.dps.StatisticsReport;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.common.model.dps.TaskErrorsInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.exception.DPSExceptionProvider;
import eu.europeana.cloud.service.dps.exception.DpsException;
import org.glassfish.jersey.client.JerseyClientBuilder;

import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
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

    private Client client = JerseyClientBuilder.newClient();

    private static final String TOPOLOGY_NAME = "TopologyName";
    private static final String TASK_ID = "TaskId";
    private static final String TASKS_URL = "/{" + TOPOLOGY_NAME + "}/tasks";
    private static final String PERMIT_TOPOLOGY_URL = "/{" + TOPOLOGY_NAME + "}/permit";
    private static final String TASK_URL = TASKS_URL + "/{" + TASK_ID + "}";
    private static final String REPORTS_RESOURCE = "reports";
    private static final String STATISTICS_RESOURCE = "statistics";

    private static final String TASK_PROGRESS_URL = TASK_URL + "/progress";
    private static final String DETAILED_TASK_REPORT_URL = TASK_URL + "/" + REPORTS_RESOURCE + "/details";
    private static final String ERRORS_TASK_REPORT_URL = TASK_URL + "/" + REPORTS_RESOURCE + "/errors";
    private static final String STATISTICS_REPORT_URL = TASK_URL + "/" + STATISTICS_RESOURCE;

    /**
     * Creates a new instance of this class.
     *
     * @param dpsUrl Url where the DPS service is located.
     *               Includes username and password to perform authenticated requests.
     */
    public DpsClient(final String dpsUrl, final String username, final String password) {

        client.register(HttpAuthenticationFeature.basic(username, password));
        this.dpsUrl = dpsUrl;
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
                System.out.println(resp);
                ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
                throw DPSExceptionProvider.generateException(errorInfo);
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
                ErrorInfo errorInfo = resp.readEntity(ErrorInfo.class);
                throw DPSExceptionProvider.generateException(errorInfo);
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
                ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
                throw DPSExceptionProvider.generateException(errorInfo);
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

    private List<SubTaskInfo> handleResponse(Response response) throws DpsException {
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(new GenericType<List<SubTaskInfo>>() {
            });
        } else {
            LOGGER.error("Task detailed report cannot be read");
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw DPSExceptionProvider.generateException(errorInfo);
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

    private TaskErrorsInfo handleErrorResponse(Response response) throws DpsException {
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(TaskErrorsInfo.class);
        } else {
            LOGGER.error("Task error report cannot be read");
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw DPSExceptionProvider.generateException(errorInfo);
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
                ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
                throw DPSExceptionProvider.generateException(errorInfo);
            }

        } finally {
            closeResponse(response);
        }
    }
}



