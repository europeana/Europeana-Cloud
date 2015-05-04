package eu.europeana.cloud.client.aas.rest;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.client.filter.HttpBasicAuthFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.web.AASParamConstants;

/**
 * AAS REST API client.
 * 
 * Creates / deletes / updates ecloud Users + passwords.
 *
 */
public class AASClient {

	private Client client = JerseyClientBuilder.newClient();
	
	/** Where the AAS lives, eats and sleeps at night. */
	private String aasUrl;

	private static final Logger LOGGER = LoggerFactory
			.getLogger(AASClient.class);

	/**
	 * Starts an AASClient.
	 * 
	 * Username / password must refer to a user with admin access
	 * as all AAS operations are blocked for normal ecloud users.
     */
	public AASClient(final String aasUrl, final String username,
			final String password) {
		
		LOGGER.info("AASClient starting...");

		client.register(new HttpBasicAuthFilter(username, password));
		this.aasUrl = aasUrl;
		
		LOGGER.info("AASClient started successfully.");
	}

	/**
	 * Creates an eCloud user with the specified username, password.
	 */
	public void createEcloudUser(final String username, final String password)
			throws CloudException {
		
        Response resp = client.target(aasUrl + "/create-user")
                .queryParam(AASParamConstants.P_USER_NAME, username)
                .queryParam(AASParamConstants.P_PASSWORD, password).request()
                .post(null);
		
		if (resp.getStatus() == Status.OK.getStatusCode()) {
			LOGGER.debug("createEcloudUser: user {} is now part of ecloud", username);
		} else {
			throw new RuntimeException("createEcloudUser() failed!");
		}
	}

	/**
	 * Updates an eCloud user with the specified username, password.
	 */
	public void updateEcloudUser(final String username, final String password)
			throws CloudException {
		
        Response resp = client.target(aasUrl + "/update-user")
                .queryParam(AASParamConstants.P_USER_NAME, username)
                .queryParam(AASParamConstants.P_PASSWORD, password).request()
                .post(null);
		
		if (resp.getStatus() == Status.OK.getStatusCode()) {
			LOGGER.debug("updateEcloudUser: user {} updated!", username);
		} else {
			throw new RuntimeException("updateEcloudUser() failed!");
		}
	}
	
	/**
	 * Deletes an eCloud user.
	 */
	public void deleteEcloudUser(final String username)
			throws CloudException {
		
        Response resp = client.target(aasUrl + "/delete-user")
                .queryParam(AASParamConstants.P_USER_NAME, username).request()
                .post(null);
		
		if (resp.getStatus() == Status.OK.getStatusCode()) {
			LOGGER.debug("deleteEcloudUser: user {} deleted!", username);
		} else {
			throw new RuntimeException("deleteEcloudUser() failed!");
		}
	}
}
