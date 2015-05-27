package eu.europeana.cloud.client.dps.rest;

import eu.europeana.cloud.service.dps.DpsTask;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.client.filter.HttpBasicAuthFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class DpsClient {

	private Logger LOGGER = LoggerFactory.getLogger(DpsClient.class);
	
	private String dpsUrl;
    
	private Client client = JerseyClientBuilder.newClient();
	
	/** TODO */
	private static final String DUMMY_TOPOLOGY = "xslt_topology";

	private static final String create = "/tasks";

	private static final String TOPOLOGY_NAME = "TopologyName";
	private static final String TASK_ID = "TaskId";
	private static final String TASKS_URL = "/topologies/{" + TOPOLOGY_NAME + "}/tasks";
	private static final String TASK_URL = TASKS_URL + "/{" + TASK_ID + "}";

	public static final String TASK_PROGRESS_URL = TASK_URL + "/progress";
	public static final String TASK_NOTIFICATION_URL = TASK_URL + "/notification";
	
	public DpsClient(final String dpsUrl, final String username, final String password)  {
		
        client.register(new HttpBasicAuthFilter(username, password));
		this.dpsUrl = dpsUrl;
	}

	public void submitTask(DpsTask task, String topologyName) {

		Response resp = client.target(dpsUrl)
				.path(TASKS_URL)
				.resolveTemplate(TOPOLOGY_NAME, topologyName)
				.request()
				.post(Entity.json(task));

		if (resp.getStatus() != Response.Status.CREATED.getStatusCode()) {
			throw new RuntimeException("submiting taks failed!!");
		}
	}
	
	public DpsTask getTask(String topologyName, long taskId) {
		
		Response getResponse = client
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
	}
	
	public String getTaskProgress(String topologyName, final long taskId) {

		Response getResponse = client
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
	}
	
	public String getTaskNotification(final String topologyName, final long taskId) {

		Response getResponse = client
				.target(dpsUrl)
				.path(TASK_NOTIFICATION_URL)
				.resolveTemplate(TOPOLOGY_NAME, topologyName)
				.resolveTemplate(TASK_ID, taskId)
				.request().get();

		if(getResponse.getStatus() == Response.Status.OK.getStatusCode()){
			String taskProgress = getResponse.readEntity(String.class);
			return taskProgress;
		}else{
			LOGGER.error("Task motification cannot be read");
			throw new RuntimeException();
		}
	}
}
