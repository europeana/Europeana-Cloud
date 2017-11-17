package eu.europeana.cloud.client.dps.rest;

import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.DpsTask;
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
import java.util.List;

/**
 * The REST API client for the Data Processing service.
 */
public class DpsClient {

    private Logger LOGGER = LoggerFactory.getLogger(DpsClient.class);

    private String dpsUrl;

    private Client client = JerseyClientBuilder.newClient();

    private static final String USERNAME = "Username";
    private static final String TOPOLOGY_NAME = "TopologyName";
    private static final String TASK_ID = "TaskId";
    private static final String TASKS_URL = "/topologies/{" + TOPOLOGY_NAME + "}/tasks";
    private static final String PERMIT_TOPOLOGY_URL = "/topologies/{" + TOPOLOGY_NAME + "}/permit";
    private static final String TASK_URL = TASKS_URL + "/{" + TASK_ID + "}";
    public static final String REPORTS_RESOURCE = "reports";

    public static final String TASK_PROGRESS_URL = TASK_URL + "/progress";
    public static final String DETAILED_TASK_REPORT_URL = TASK_URL + "/" + REPORTS_RESOURCE + "/details";

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
    public void submitTask(DpsTask task, String topologyName) {

        Response resp = null;
        try {
            resp = client.target(dpsUrl)
                    .path(TASKS_URL)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName)
                    .request()
                    .post(Entity.json(task));

            if (resp.getStatus() != Response.Status.CREATED.getStatusCode()) {
                throw new RuntimeException("submitting task failed!!");
            }

        } finally {
            closeResponse(resp);
        }
    }

    /**
     * Submits a task for execution in the specified topology.
     */
    public void topologyPermit(String topologyName, String username) {
        Form form = new Form();
        form.param("username", username);
        Response resp = null;

        try {
            resp = client.target(dpsUrl)
                    .path(PERMIT_TOPOLOGY_URL)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName)
                    .request()
                    .post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));


            if (resp.getStatus() != Response.Status.OK.getStatusCode()) {
                //TODO exception wrapping should be implemented
                throw new RuntimeException("Permit topology failed!");
            }
        } finally {
            closeResponse(resp);
        }
    }


    public DpsTask getTask(String topologyName, long taskId) {

        Response getResponse = null;
        try {
            getResponse = client
                    .target(dpsUrl)
                    .path(TASK_URL)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName)
                    .resolveTemplate(TASK_ID, String.valueOf(taskId))
                    .request()
                    .header("Accept", MediaType.APPLICATION_JSON)
                    .get();

            if (getResponse.getStatus() == Response.Status.OK.getStatusCode()) {
                DpsTask task = getResponse.readEntity(DpsTask.class);
                return task;
            } else {
                throw new RuntimeException();
            }
        } finally {
            closeResponse(getResponse);
        }
    }

    /**
     * Retrieves progress for the specified combination of taskId and topology.
     */
    public TaskInfo getTaskProgress(String topologyName, final long taskId) {

        Response response = null;

        try {
            response = client
                    .target(dpsUrl)
                    .path(TASK_PROGRESS_URL)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName)
                    .resolveTemplate(TASK_ID, taskId)
                    .request().get();

            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                TaskInfo taskInfo = response.readEntity(TaskInfo.class);
                return taskInfo;
            } else {
                LOGGER.error("Task progress cannot be read");
                throw new RuntimeException();
            }
        } finally {
            closeResponse(response);
        }
    }

    public List<SubTaskInfo> getDetailedTaskReport(final String topologyName, final long taskId) {

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

    public List<SubTaskInfo> getDetailedTaskReportBetweenChunks(final String topologyName, final long taskId,int from,int to) {

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

    private List<SubTaskInfo> handleResponse(Response response) {
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            List<SubTaskInfo> subTaskInfoList = response.readEntity(new GenericType<List<SubTaskInfo>>() {
            });
            return subTaskInfoList;
        } else {
            LOGGER.error("Task detailed report cannot be read");
            throw new RuntimeException();
        }
    }

    private void closeResponse(Response response) {
        if (response != null) {
            response.close();
        }
    }
}



