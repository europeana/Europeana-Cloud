package eu.europeana.cloud.service.dps.rest;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.rest.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidationException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.validation.constraints.NotNull;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.Arrays;

import static org.junit.Assert.fail;

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
    
    @Autowired
    private RecordServiceClient recordServiceClient;

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
    private DpsTask TASK_1;
    private DpsTask TASK_WITH_MALFORMED_URL;

    private UriInfo URI_INFO;
    
    private static final String AUTH_HEADER_VALUE = "header_value";

    @Before
    public void mockUp() throws Exception {

		TASK = new DpsTask("xsltTask");
        TASK.addDataEntry("FILE_URLS", Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        TASK.getTaskId();

        TASK_1 = new DpsTask("xsltTask");
        TASK_1.addDataEntry("FILE_URLS", Arrays.asList("http://127.0.0.1:8080/mcs/records/sampleId/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        
        TASK_WITH_MALFORMED_URL = new DpsTask("taskWithMalformedUrl");
        TASK_WITH_MALFORMED_URL.addDataEntry("FILE_URLS", Arrays.asList("httpz://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
		
        URI_INFO = Mockito.mock(UriInfo.class);
        Mockito.doReturn(PROGRESS).when(reportService).getTaskProgress(Mockito.anyString());
        Mockito.when(URI_INFO.getBaseUri()).thenReturn(new URI("http:127.0.0.1:8080/sampleuri/"));
        Mockito.when(topologyManager.containsTopology(SAMPLE_TOPOLOGY_NAME)).thenReturn(true);
        Mockito.when(topologyManager.getNameToUserMap()).thenReturn(ImmutableMap.of(SAMPLE_TOPOLOGY_NAME, "userName"));

        Mockito.when(recordServiceClient.useAuthorizationHeader(Mockito.anyString())).thenReturn(recordServiceClient);
    }

    /*
        Task Submission tests
     */
    @Test(expected = AuthenticationCredentialsNotFoundException.class)
    public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToSubmitTask() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {

        DpsTask t = new DpsTask("xsltTask");
        String topology = "xsltTopology";

        topologyTasksResource.submitTask(t, topology, URI_INFO, AUTH_HEADER_VALUE);
    }

    @Test
    public void shouldBeAbleToSubmitTaskToTopologyThatHasPermissionsTo() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(VAN_PERSIE, SAMPLE_TOPOLOGY_NAME);
        logoutEveryone();
        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
        DpsTask sampleTask = new DpsTask();
        topologyTasksResource.submitTask(sampleTask, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
    }

    @Test(expected = AccessDeniedException.class)
    public void shouldNotBeAbleToSubmitTaskToTopologyThatHasNotPermissionsTo() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(VAN_PERSIE, SAMPLE_TOPOLOGY_NAME);
        logoutEveryone();
        login(RONALDO, RONALD_PASSWORD);
        DpsTask sampleTask = new DpsTask();
        topologyTasksResource.submitTask(sampleTask, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
    }

    // -- progress report tests --

    @Test
    public void shouldBeAbleToCheckProgressIfHeIsTheTaskOwner() throws AccessDeniedOrObjectDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {

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
    public void vanPersieShouldNotBeAbleCheckProgressOfRonaldosTask() throws AccessDeniedOrObjectDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {

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
    public void vanPersieShouldNotBeAbleSubmitTaskToNotDefinedTopology() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {
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
    public void vanPersieShouldNotBeAbleGetTaskProgressToNotDefinedTopology() throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {
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
    public void vanPersieShouldNotBeAbleKillTaskoNotDefinedTopology() throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {
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
    public void vanPersieShouldNotBeAbleCheckKillFlagNotDefinedTopology() throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {
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
    public void vanPersieShouldNotBeRemoveKillFlagNotDefinedTopology() throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {
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
    
    @Test(expected = RuntimeException.class)
    public void runtimeExceptionShouldBeThrownWhenMCSIsNotAvailable() throws MCSException, AccessDeniedOrTopologyDoesNotExistException, TaskSubmissionException, DpsTaskValidationException {

        Mockito.doThrow(new RuntimeException()).when(recordServiceClient).grantPermissionsToVersion(Mockito.matches("sampleId"), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(Permission.class));

        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);
        login(RONALDO, RONALD_PASSWORD);
        topologyTasksResource.submitTask(TASK_1, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);

    }

    @Test(expected = TaskSubmissionException.class)
    public void exceptionShouldBeThrownWhenMCSIsNotAvailable() throws MCSException, AccessDeniedOrTopologyDoesNotExistException, TaskSubmissionException, DpsTaskValidationException {

        Mockito.doThrow(new MCSException()).when(recordServiceClient).grantPermissionsToVersion(Mockito.matches("sampleId"), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(Permission.class));

        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);
        login(RONALDO, RONALD_PASSWORD);
        topologyTasksResource.submitTask(TASK_1, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
    }

    @Test(expected = TaskSubmissionException.class)
    public void exceptionShouldBeThrownWhenTaskUrlIsMalformed() throws MCSException, AccessDeniedOrTopologyDoesNotExistException, TaskSubmissionException, DpsTaskValidationException {

        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);
        login(RONALDO, RONALD_PASSWORD);
        topologyTasksResource.submitTask(TASK_WITH_MALFORMED_URL, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
    }
    
    
    
}

