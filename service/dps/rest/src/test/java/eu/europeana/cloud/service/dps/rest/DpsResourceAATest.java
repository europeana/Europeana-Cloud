package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.net.URI;

@RunWith(SpringJUnit4ClassRunner.class)
public class DpsResourceAATest extends AbstractSecurityTest {

    @Autowired
    @NotNull
    private TopologyTasksResource topologyTasksResource;

    @Autowired
    @NotNull
    private TopologiesResource topologiesResource;

	@Autowired
	@NotNull
	private TaskExecutionReportService reportService;

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
	private final static String PROGRESS = "100%";
	private DpsTask TASK;

    @Before
    public void mockUp() throws Exception {

		TASK = new DpsTask("xsltTask");
		TASK.getTaskId();
		
		 Mockito.doReturn(PROGRESS).when(reportService).getTaskProgress(Mockito.anyString());
    }

    /*
        Task Submission tests
     */
    @Test(expected = AuthenticationCredentialsNotFoundException.class)
    public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToSubmitTask() {

        DpsTask t = new DpsTask("xsltTask");
        String topology = "xsltTopology";

        topologyTasksResource.submitTask(t, topology);
    }

    @Test
    public void shouldBeAbleToSubmitTaskToTopologyThatHasPermissionsTo() {
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.assignPermissionsToTopology(VAN_PERSIE, SAMPLE_TOPOLOGY_NAME);
        logoutEveryone();
        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
        DpsTask sampleTask = new DpsTask();
        topologyTasksResource.submitTask(sampleTask, SAMPLE_TOPOLOGY_NAME);
    }
    
    @Test(expected = AccessDeniedException.class)
    public void shouldNotBeAbleToSubmitTaskToTopologyThatHasNotPermissionsTo() {
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.assignPermissionsToTopology(VAN_PERSIE, SAMPLE_TOPOLOGY_NAME);
        logoutEveryone();
        login(RONALDO, RONALD_PASSWORD);
        DpsTask sampleTask = new DpsTask();
        topologyTasksResource.submitTask(sampleTask, SAMPLE_TOPOLOGY_NAME);
    }

    // -- progress report tests -- //
   
    @Test
	public void shouldBeAbleToCheckProgressIfHeIsTheTaskOwner() throws AccessDeniedOrObjectDoesNotExistException {
    	
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.assignPermissionsToTopology(VAN_PERSIE, SAMPLE_TOPOLOGY_NAME);

        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
        topologyTasksResource.submitTask(TASK, SAMPLE_TOPOLOGY_NAME);
        topologyTasksResource.getTaskProgress("" + TASK.getTaskId());
	}

	@Test(expected = AuthenticationCredentialsNotFoundException.class)
	public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToCheckProgress() throws AccessDeniedOrObjectDoesNotExistException {

		topologyTasksResource.getTaskProgress("" + TASK.getTaskId());
	}

    
    @Test(expected = AccessDeniedException.class)
	public void vanPersieShouldNotBeAbleCheckProgressOfRonaldosTask() throws AccessDeniedOrObjectDoesNotExistException {

        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.assignPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);
		
        login(RONALDO, RONALD_PASSWORD);
        topologyTasksResource.submitTask(TASK, SAMPLE_TOPOLOGY_NAME);
		
        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
        topologyTasksResource.getTaskProgress("" + TASK.getTaskId());
	}
    
}
