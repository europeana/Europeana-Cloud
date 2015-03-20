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
    private TaskExecutionReportService dpsService;

    private static final String GLOBAL_ID = "GLOBAL_ID";

    private UriInfo URI_INFO;

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

    @Before
    public void mockUp() throws Exception {

        URI_INFO = Mockito.mock(UriInfo.class);
        UriBuilder uriBuilder = Mockito.mock(UriBuilder.class);

        Mockito.doReturn(uriBuilder).when(URI_INFO).getBaseUriBuilder();
        Mockito.doReturn(uriBuilder).when(uriBuilder)
                .path((Class) Mockito.anyObject());
        Mockito.doReturn(new URI("")).when(uriBuilder)
                .buildFromMap(Mockito.anyMap());
        Mockito.doReturn(new URI("")).when(uriBuilder)
                .buildFromMap(Mockito.anyMap());
        Mockito.doReturn(new URI("")).when(URI_INFO)
                .resolve((URI) Mockito.anyObject());

        // Mockito.doReturn(record).when(recordService).getRecord(Mockito.anyString());
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

//    @Test //TODO: does not work because of problems with Kafka communication
    public void shouldBeAbleToReadProgressOfSubmittedTask() throws AccessDeniedOrObjectDoesNotExistException {
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.assignPermissionsToTopology(VAN_PERSIE, SAMPLE_TOPOLOGY_NAME);
        logoutEveryone();
        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
        DpsTask sampleTask = new DpsTask();
        topologyTasksResource.submitTask(sampleTask, SAMPLE_TOPOLOGY_NAME);
        topologyTasksResource.getTaskProgress(sampleTask.getTaskId()+"");
    }

    @Test(expected = AccessDeniedException.class)
    public void shouldNotBeAbleToReadProgressOfTaskThatIsNotOwnedByUser() throws AccessDeniedOrObjectDoesNotExistException {
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.assignPermissionsToTopology(VAN_PERSIE, SAMPLE_TOPOLOGY_NAME);
        logoutEveryone();
        login(RONALDO, RONALD_PASSWORD);
        DpsTask sampleTask = new DpsTask();
        topologyTasksResource.submitTask(sampleTask, SAMPLE_TOPOLOGY_NAME);
        topologyTasksResource.getTaskProgress(sampleTask.getTaskId()+"");
    }
    
    
}
