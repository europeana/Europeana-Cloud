package eu.europeana.cloud.client.aas.rest.web;

import eu.europeana.cloud.client.aas.rest.AASClient;
import eu.europeana.cloud.client.aas.rest.CloudException;
import org.junit.Ignore;
import org.junit.Test;

/**
 * This class was made with intention to have easy way to run rest requests to DPS application.<br/>
 * This is intentionally annotated with @Ignore annotation because it should be ran by hand by the developer.<br/>
 * In the future we will extend it by adding more integration test. Probably we will also create separate module for
 * integration tests.<br/>
 */
@Ignore
public class AASClientTestIT {

    private static final String AAS_LOCATION = "http://127.0.0.1:8080/aas";
    private static final String USER = "admin";
    private static final String PASSWORD = "glEumLWDSVUjQcRVswhN";

    @Test
    public void shouldCreateUser() throws CloudException {
        AASClient client = new AASClient(AAS_LOCATION, USER, PASSWORD);
        client.createEcloudUser("testUser2", "testUserPassword2");
    }

    @Test
    public void shouldUpdateUser() throws CloudException {
        AASClient client = new AASClient(AAS_LOCATION, USER, PASSWORD);
        client.updateEcloudUser("testUser1","testUserPassword2");
    }


    @Test
    public void shouldDeleteUser() throws CloudException {
        AASClient client = new AASClient(AAS_LOCATION, USER, PASSWORD);
        client.deleteEcloudUser("testUser2");
    }
}
