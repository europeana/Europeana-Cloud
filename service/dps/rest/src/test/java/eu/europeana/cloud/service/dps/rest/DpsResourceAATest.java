package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
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

import static org.junit.Assert.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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

    @Autowired
    @NotNull
    private TopologyManager topologyManager;

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

    private UriInfo URI_INFO;
    
    private static final String AUTH_HEADER_VALUE = "header_value";

    @Before
    public void mockUp() throws Exception {

		TASK = new DpsTask("xsltTask");
		TASK.getTaskId();
		
        URI_INFO = Mockito.mock(UriInfo.class);
        Mockito.doReturn(PROGRESS).when(reportService).getTaskProgress(Mockito.anyString());
        Mockito.when(URI_INFO.getBaseUri()).thenReturn(new URI("http:127.0.0.1:8080/sampleuri/"));
        Mockito.when(topologyManager.containsTopology(SAMPLE_TOPOLOGY_NAME)).thenReturn(true);
    }

    /*
        Task Submission tests
     */
    @Test(expected = AuthenticationCredentialsNotFoundException.class)
    public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToSubmitTask() throws AccessDeniedOrTopologyDoesNotExistException {

        DpsTask t = new DpsTask("xsltTask");
        String topology = "xsltTopology";

        topologyTasksResource.submitTask(t, topology, URI_INFO, AUTH_HEADER_VALUE);
    }

    @Test
    public void shouldBeAbleToSubmitTaskToTopologyThatHasPermissionsTo() throws AccessDeniedOrTopologyDoesNotExistException {
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(VAN_PERSIE, SAMPLE_TOPOLOGY_NAME);
        logoutEveryone();
        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
        DpsTask sampleTask = new DpsTask();
        topologyTasksResource.submitTask(sampleTask, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
    }

    @Test(expected = AccessDeniedException.class)
    public void shouldNotBeAbleToSubmitTaskToTopologyThatHasNotPermissionsTo() throws AccessDeniedOrTopologyDoesNotExistException {
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(VAN_PERSIE, SAMPLE_TOPOLOGY_NAME);
        logoutEveryone();
        login(RONALDO, RONALD_PASSWORD);
        DpsTask sampleTask = new DpsTask();
        topologyTasksResource.submitTask(sampleTask, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
    }

    // -- progress report tests --

    @Test
    public void shouldBeAbleToCheckProgressIfHeIsTheTaskOwner() throws AccessDeniedOrObjectDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException {

        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(VAN_PERSIE, SAMPLE_TOPOLOGY_NAME);

        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
        topologyTasksResource.submitTask(TASK, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
        topologyTasksResource.getTaskProgress(SAMPLE_TOPOLOGY_NAME, "" + TASK.getTaskId());
    }

    @Test(expected = AuthenticationCredentialsNotFoundException.class)
    public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToCheckProgress() throws AccessDeniedOrObjectDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException {

		topologyTasksResource.getTaskProgress(SAMPLE_TOPOLOGY_NAME, "" + TASK.getTaskId());
	}

  

    @Test(expected = AccessDeniedException.class)
    public void vanPersieShouldNotBeAbleCheckProgressOfRonaldosTask() throws AccessDeniedOrObjectDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException {

        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);
		
        login(RONALDO, RONALD_PASSWORD);
        topologyTasksResource.submitTask(TASK, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
        topologyTasksResource.getTaskProgress(SAMPLE_TOPOLOGY_NAME, "" + TASK.getTaskId());
    }

    @Test(expected = AccessDeniedOrTopologyDoesNotExistException.class)
    public void vanPersieShouldNotBeAbleGrantPermissionsToNotDefinedTopology() throws AccessDeniedOrObjectDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException {
        final String FAIL_TOPOLOGY_NAME = "failTopology";
        //given
        login(ADMIN, ADMIN_PASSWORD);

        //when
        topologiesResource.grantPermissionsToTopology(RONALDO, FAIL_TOPOLOGY_NAME);

        //then - intentionally empty
    }

    @Test
    public void vanPersieShouldNotBeAbleSubmitTaskToNotDefinedTopology() throws AccessDeniedOrTopologyDoesNotExistException {
        //given

        Mockito.reset(topologyManager);
        Mockito.when(topologyManager.containsTopology(SAMPLE_TOPOLOGY_NAME)).thenReturn(true, false);

        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);
        login(RONALDO, RONALD_PASSWORD);
        //when
        try {
            topologyTasksResource.submitTask(TASK, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
            fail();
            //then
        } catch (AccessDeniedOrTopologyDoesNotExistException e) {
        }
    }

    @Test
    public void vanPersieShouldNotBeAbleGetTaskProgressToNotDefinedTopology() throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
        //given

        Mockito.reset(topologyManager);
        Mockito.when(topologyManager.containsTopology(SAMPLE_TOPOLOGY_NAME)).thenReturn(true, true, false);

        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);
        login(RONALDO, RONALD_PASSWORD);
        topologyTasksResource.submitTask(TASK, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
        //when
        try {
            topologyTasksResource.getTaskProgress(SAMPLE_TOPOLOGY_NAME, "" + TASK.getTaskId());
            fail();
            //then
        } catch (AccessDeniedOrTopologyDoesNotExistException e) {
        }
    }



    @Test
    public void vanPersieShouldNotBeAbleKillTaskoNotDefinedTopology() throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
        //given
        Mockito.reset(topologyManager);
        Mockito.when(topologyManager.containsTopology(SAMPLE_TOPOLOGY_NAME)).thenReturn(true, true, false);

        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);
        login(RONALDO, RONALD_PASSWORD);
        topologyTasksResource.submitTask(TASK, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
        //when
        try {
            topologyTasksResource.killTask(SAMPLE_TOPOLOGY_NAME, "" + TASK.getTaskId());
            fail();
            //then
        } catch (AccessDeniedOrTopologyDoesNotExistException e) {
        }
    }


    @Test
    public void vanPersieShouldNotBeAbleCheckKillFlagNotDefinedTopology() throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
        //given
        Mockito.reset(topologyManager);
        Mockito.when(topologyManager.containsTopology(SAMPLE_TOPOLOGY_NAME)).thenReturn(true, true, false);

        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);
        login(RONALDO, RONALD_PASSWORD);
        topologyTasksResource.submitTask(TASK, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
        //when
        try {
            topologyTasksResource.checkKillFlag(SAMPLE_TOPOLOGY_NAME, "" + TASK.getTaskId());
            fail();
            //then
        } catch (AccessDeniedOrTopologyDoesNotExistException e) {
        }
    }

    @Test
    public void vanPersieShouldNotBeRemoveKillFlagNotDefinedTopology() throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException {
        //given

        Mockito.reset(topologyManager);
        Mockito.when(topologyManager.containsTopology(SAMPLE_TOPOLOGY_NAME)).thenReturn(true, true, false);

        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);
        login(RONALDO, RONALD_PASSWORD);
        topologyTasksResource.submitTask(TASK, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
        //when
        try {
            topologyTasksResource.removeKillFlag(SAMPLE_TOPOLOGY_NAME, "" + TASK.getTaskId());
            fail();
            //then
        } catch (AccessDeniedOrTopologyDoesNotExistException e) {
        }
    }

}

