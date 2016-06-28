package eu.europeana.cloud.client.dps.rest;

import eu.europeana.cloud.service.dps.DpsTask;
import org.glassfish.jersey.client.JerseyClientBuilder;

import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

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
    private static final String PERMIT_TIPOLOGY_URL = "/topologies/{" + TOPOLOGY_NAME + "}/permit";
	private static final String TASK_URL = TASKS_URL + "/{" + TASK_ID + "}";

	public static final String TASK_PROGRESS_URL = TASK_URL + "/progress";
	public static final String TASK_NOTIFICATION_URL = TASK_URL + "/notification";

    /**
     * Creates a new instance of this class.
     *
     * @param dpsUrl Url where the DPS service is located.
     * Includes username and password to perform authenticated requests.
     */
	public DpsClient(final String dpsUrl, final String username, final String password)  {
		
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
				throw new RuntimeException("submiting taks failed!!");
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
        form.param("userneme", username);
		Response resp = null;

		try {
			resp = client.target(dpsUrl)
					.path(PERMIT_TIPOLOGY_URL)
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

			if(getResponse.getStatus() == Response.Status.OK.getStatusCode()){
				DpsTask task = getResponse.readEntity(DpsTask.class);
				return task;
			}else{
				throw new RuntimeException();
			}
		} finally {
			closeResponse(getResponse);
		}
	}

	/**
	 * Retrieves progress for the specified combination of taskId and topology.
	 */
	public String getTaskProgress(String topologyName, final long taskId) {

		Response getResponse = null;
		
		try{
			getResponse = client
					.target(dpsUrl)
					.path(TASK_PROGRESS_URL)
					.resolveTemplate(TOPOLOGY_NAME, topologyName)
					.resolveTemplate(TASK_ID, taskId)
					.request().get();

			if(getResponse.getStatus() == Response.Status.OK.getStatusCode()){
				String taskProgress = getResponse.readEntity(String.class);
				return taskProgress;
			}else{
				LOGGER.error("Task progress cannot be read");
				throw new RuntimeException();
			}
		}finally {
			closeResponse(getResponse);
		}
	}
	
	public String getTaskNotification(final String topologyName, final long taskId) {

		Response getResponse = null;

        try {
            getResponse = client
                    .target(dpsUrl)
                    .path(TASK_NOTIFICATION_URL)
                    .resolveTemplate(TOPOLOGY_NAME, topologyName)
                    .resolveTemplate(TASK_ID, taskId)
                    .request().get();

            if (getResponse.getStatus() == Response.Status.OK.getStatusCode()) {
                String taskProgress = getResponse.readEntity(String.class);
                return taskProgress;
            } else {
                LOGGER.error("Task notification cannot be read");
                throw new RuntimeException();
            }

        } finally {
            closeResponse(getResponse);
        }
    }
	
	private void closeResponse(Response response) {
		if (response != null) {
			response.close();
		}
	}
}
