package eu.europeana.cloud.client.aas.rest;

import eu.europeana.cloud.common.web.AASParamConstants;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

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

		client.register(HttpAuthenticationFeature.basic(username, password));
		this.aasUrl = aasUrl;
		
		LOGGER.info("AASClient started successfully.");
	}

	/**
	 * Creates an eCloud user with the specified username, password.
	 */
	public void createEcloudUser(final String username, final String password)
			throws CloudException {
		
		Response resp = null;
		try {
			resp = client.target(aasUrl + "/create-user")
					.queryParam(AASParamConstants.P_USER_NAME, username)
					.queryParam(AASParamConstants.P_PASS_TOKEN, password).request()
					.post(null);

			if (resp.getStatus() == Status.OK.getStatusCode()) {
				LOGGER.debug("createEcloudUser: user {} is now part of ecloud", username);
			} else {
				throw new RuntimeException("createEcloudUser() failed!");
			}
		} finally {
            closeResponse(resp);
		}
	}

	/**
	 * Updates an eCloud user with the specified username, password.
	 */
	public void updateEcloudUser(final String username, final String password)
			throws CloudException {
		
		Response resp = null;
		try {
			resp = client.target(aasUrl + "/update-user")
					.queryParam(AASParamConstants.P_USER_NAME, username)
					.queryParam(AASParamConstants.P_PASS_TOKEN, password).request()
					.post(null);

			if (resp.getStatus() == Status.OK.getStatusCode()) {
				LOGGER.debug("updateEcloudUser: user {} updated!", username);
			} else {
				throw new RuntimeException("updateEcloudUser() failed!");
			}
		} finally {
            closeResponse(resp);
		}
	}
	
	/**
	 * Deletes an eCloud user.
	 */
	public void deleteEcloudUser(final String username)
			throws CloudException {

		Response resp = null;
		try {
			resp = client.target(aasUrl + "/delete-user")
					.queryParam(AASParamConstants.P_USER_NAME, username).request()
					.post(null);

			if (resp.getStatus() == Status.OK.getStatusCode()) {
				LOGGER.debug("deleteEcloudUser: user {} deleted!", username);
			} else {
				throw new RuntimeException("deleteEcloudUser() failed!");
			}
		} finally {
            closeResponse(resp);
		}
	}

    private void closeResponse(Response response) {
        if (response != null) {
            response.close();
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
