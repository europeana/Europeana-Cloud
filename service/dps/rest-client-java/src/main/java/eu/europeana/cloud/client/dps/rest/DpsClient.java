package eu.europeana.cloud.client.dps.rest;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.client.filter.HttpBasicAuthFilter;

import eu.europeana.cloud.service.dps.DpsTask;

public class DpsClient {

	private String dpsUrl;
    
	private Client client = JerseyClientBuilder.newClient();
	
	/** TODO */
	private static final String DUMMY_TOPOLOGY = "xslt_topology";

	private static final String create = "/tasks";

	private static final String getTaskNotification = "/tasks" + "/{" + "taskId" + "}" + "/notification";
	private static final String getTaskProgress = "/tasks" + "/{" + "taskId" + "}" + "/progress";
	private static final String get = "/tasks" + "/{" + "type" + "}";
	
	public DpsClient(final String dpsUrl, final String username, final String password)  {
		
        client.register(new HttpBasicAuthFilter(username, password));
		this.dpsUrl = dpsUrl;
	}
	
	public void submitTask(DpsTask task) {

		Response resp = client.target(dpsUrl)
				.path(create).request()
				.post(Entity.json(task));
	}
	
	public DpsTask getTask() {

		Response getResponse = client
				.target(dpsUrl)
				.path(get)
				.resolveTemplate("type",
						DUMMY_TOPOLOGY).request().get();

		DpsTask getTask = getResponse.readEntity(DpsTask.class);
		return getTask;
	}
	

	public String getTaskProgress(final String taskId) {

		Response getResponse = client
				.target(dpsUrl)
				.path(getTaskProgress)
				.resolveTemplate("taskId",
						taskId).request().get();

		String taskProgress = getResponse.readEntity(String.class);
		return taskProgress;
	}
	

	public String getTaskNotification(final String taskId) {

		Response getResponse = client
				.target(dpsUrl)
				.path(getTaskNotification)
				.resolveTemplate("taskId",
						taskId).request().get();

		String taskProgress = getResponse.readEntity(String.class);
		return taskProgress;
	}
}
