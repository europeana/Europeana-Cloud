package eu.europeana.cloud.client.aas.rest.web;

import org.junit.Test;

import eu.europeana.cloud.client.aas.rest.AASClient;
import eu.europeana.cloud.client.aas.rest.CloudException;

public class AASClientTest {

	private final static String AAS_BASE_URL = "http://localhost:8081/ecloud-service-aas-rest-0.3-SNAPSHOT";

	//	Not a real test, just playing around
//    @Test
	public void AASClientTest() throws CloudException {

		AASClient aasClient = new AASClient(AAS_BASE_URL, "Rolando", "Rolando");

		aasClient.createEcloudUser("test2", "test2");
		aasClient.updateEcloudUser("test2", "test3");
		aasClient.deleteEcloudUser("test2");
	}

}
