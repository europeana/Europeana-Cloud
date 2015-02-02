package eu.europeana.cloud.client.dps.rest;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.JerseyClientBuilder;

import eu.europeana.cloud.service.dps.DpsTask;

public class DpsClient {

//	 private static final String BASE_URL =  "http://146.48.82.158:8080/";
	private static final String BASE_URL = "http://localhost:8080/ecloud-service-dps-rest";
    
	private Client client = JerseyClientBuilder.newClient();

	private static final String create = "/tasks";
	private static final String get = "/tasks" + "/{" + "type" + "}";
	
	public void DpsClient()  {

	}
	
	public void submitTask(DpsTask task) {

		Response resp = client.target(BASE_URL)
				.path(create).request()
				.post(Entity.json(task));
	}
	
	public DpsTask fetchTask() {

		Response getResponse = client
				.target(BASE_URL)
				.path(get)
				.resolveTemplate("type",
						"xslt").request().get();

		DpsTask getTask = getResponse.readEntity(DpsTask.class);
		return getTask;
	}
}
