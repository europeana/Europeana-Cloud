package eu.europeana.cloud.service.dps.rest;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.validation.constraints.NotNull;

@RunWith(SpringJUnit4ClassRunner.class)
public class TopologiesResourceAATest extends AbstractSecurityTest {

    @Autowired
    @NotNull
    private TopologiesResource topologiesResource;

    /**
     * Pre-defined users
     */
    private final static String RANDOM_PERSON = "Cristiano";
    private final static String RANDOM_PASSWORD = "Ronaldo";

    private final static String VAN_PERSIE = "Robin_Van_Persie";
    private final static String VAN_PERSIE_PASSWORD = "Feyenoord";
    
    private final static String RONALDO = "Cristiano";
    private final static String RONALD_PASSWORD = "Ronaldo";

    private final static String ADMIN = "admin";
    private final static String ADMIN_PASSWORD = "admin";

    private final static String SAMPLE_TOPOLOGY_NAME = "sampleTopology";


    @Test
    public void shouldBeAbleToAssignPermissionsWhenAdmin() {

        login(ADMIN, ADMIN_PASSWORD);
        String topology = "xsltTopology";
        topologiesResource.grantPermissionsToTopology("krystian", topology);
    }

    @Test(expected = AuthenticationCredentialsNotFoundException.class)
    public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToAssignPermissionsToTopology() {
        String topology = "xsltTopology";
        topologiesResource.grantPermissionsToTopology(RANDOM_PERSON, topology);
    }

    @Test(expected = AccessDeniedException.class)
    public void shouldThrowExceptionWhenNonAdminUserTriesToAssignPermissionsToTopology() {
        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
        String topology = "xsltTopology";
        topologiesResource.grantPermissionsToTopology(RANDOM_PERSON, topology);
    }
    
    @Test
    public void shouldBeAbleToAssignMoreThanOneDifferentPermissionsToSameTopology(){
        login(ADMIN, ADMIN_PASSWORD);
        String topology = "xsltTopology";
        topologiesResource.grantPermissionsToTopology("sampleUser", topology);
        topologiesResource.grantPermissionsToTopology("sampleUser1", topology);
    }
}
