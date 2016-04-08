package eu.europeana.cloud.service.dps.rest;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.rest.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.service.utils.validation.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.validation.constraints.NotNull;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
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

    @Autowired
    private DataSetServiceClient dataSetServiceClient;

    @Autowired
    private FileServiceClient fileServiceClient;

    @Autowired
    private CassandraTaskInfoDAO taskDAO;

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
    private DpsTask XSLT_TASK;
    private DpsTask XSLT_TASK2;
    private DpsTask XSLT_TASK_WITH_MALFORMED_URL;
    private DpsTask IC_TASK;

    private UriInfo URI_INFO;
    private AsyncResponse asyncResponse;

    private static final String AUTH_HEADER_VALUE = "header_value";


    @Before
    public void mockUp() throws Exception {

        XSLT_TASK = new DpsTask("xsltTask");
        XSLT_TASK.addDataEntry(DpsTask.FILE_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));

        XSLT_TASK2 = new DpsTask("xsltTask");
        XSLT_TASK2.addDataEntry(DpsTask.FILE_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/records/sampleId/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));

        XSLT_TASK_WITH_MALFORMED_URL = new DpsTask("taskWithMalformedUrl");
        XSLT_TASK_WITH_MALFORMED_URL.addDataEntry("FILE_URLS", Arrays.asList("httpz://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));


        URI_INFO = Mockito.mock(UriInfo.class);
        asyncResponse = Mockito.mock(AsyncResponse.class);
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

        topologyTasksResource.submitTask(asyncResponse, t, topology, URI_INFO, AUTH_HEADER_VALUE);
    }

    @Test
    public void shouldBeAbleToSubmitTaskToTopologyThatHasPermissionsTo() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(VAN_PERSIE, SAMPLE_TOPOLOGY_NAME);
        logoutEveryone();
        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
        DpsTask task = new DpsTask();
        task.addDataEntry(DpsTask.FILE_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        task.addParameter(PluginParameterKeys.TASK_SUBMITTER_NAME, "some");
        task.addParameter(PluginParameterKeys.MIME_TYPE, "image/tiff");
        task.addParameter(PluginParameterKeys.OUTPUT_MIME_TYPE, "image/jp2");
        topologyTasksResource.submitTask(asyncResponse, task, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
    }

    @Test
    public void shouldBeAbleToSubmitTaskToXsltTopology() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {
        //when
        DpsTask task = new DpsTask("xsltTask");
        task.addDataEntry(DpsTask.FILE_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        task.addParameter(PluginParameterKeys.TASK_SUBMITTER_NAME, "some");
        task.addParameter(PluginParameterKeys.XSLT_URL, "http://test.xslt");
        String topologyName = "xslt_topology";
        String user = VAN_PERSIE;
        grantUserToTopology(topologyName, user);
        login(user, VAN_PERSIE_PASSWORD);
        //then
        topologyTasksResource.submitTask(asyncResponse, task, topologyName, URI_INFO, AUTH_HEADER_VALUE);
    }

    @Test
    public void shouldThrowDpsTaskValidationExceptionOnSubmitTaskToXsltTopologyWithMissingFileUrls() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {
        //when
        DpsTask task = new DpsTask("xsltTask");
        task.addParameter(PluginParameterKeys.TASK_SUBMITTER_NAME, "some");
        task.addParameter(PluginParameterKeys.XSLT_URL, "http://test.xslt");
        String topologyName = "xslt_topology";
        String user = VAN_PERSIE;
        grantUserToTopology(topologyName, user);
        login(user, VAN_PERSIE_PASSWORD);
        try {
            //when
            topologyTasksResource.submitTask(asyncResponse, task, topologyName, URI_INFO, AUTH_HEADER_VALUE);
            fail();
        } catch (DpsTaskValidationException e) {
            //then
            assertThat(e.getMessage(), startsWith("Validation failed"));
        }
    }

    @Test
    public void shouldThrowDpsTaskValidationExceptionOnSubmitTaskToXsltTopologyWithMissingXsltUrl() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {
        //when
        DpsTask task = new DpsTask("xsltTask");
        task.addDataEntry(DpsTask.FILE_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        task.addParameter(PluginParameterKeys.TASK_SUBMITTER_NAME, "some");

        String topologyName = "xslt_topology";
        String user = VAN_PERSIE;
        grantUserToTopology(topologyName, user);
        login(user, VAN_PERSIE_PASSWORD);
        try {
            //when
            topologyTasksResource.submitTask(asyncResponse, task, topologyName, URI_INFO, AUTH_HEADER_VALUE);
            fail();
        } catch (DpsTaskValidationException e) {
            //then
            assertThat(e.getMessage(), is("Expected parameter does not exist in dpsTask. Parameter name: XSLT_URL"));
        }
    }

    @Test
    public void shouldThrowDpsTaskValidationExceptionOnSubmitTaskToXsltTopologyWithMissingTaskSubmitterName() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {
        //when
        DpsTask task = new DpsTask("xsltTask");
        task.addParameter(PluginParameterKeys.XSLT_URL, "http://test.xslt");
        task.addDataEntry(DpsTask.FILE_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        String topologyName = "xslt_topology";
        String user = VAN_PERSIE;
        grantUserToTopology(topologyName, user);
        login(user, VAN_PERSIE_PASSWORD);
        try {
            //when
            topologyTasksResource.submitTask(asyncResponse, task, topologyName, URI_INFO, AUTH_HEADER_VALUE);
            fail();
        } catch (DpsTaskValidationException e) {
            //then
            assertThat(e.getMessage(), is("Expected parameter does not exist in dpsTask. Parameter name: TASK_SUBMITTER_NAME"));
        }
    }

    @Test
    public void shouldBeAbleToSubmitTaskToIcTopology() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {
        //when
        DpsTask task = new DpsTask("icTask");
        task.addDataEntry(DpsTask.FILE_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        task.addParameter(PluginParameterKeys.TASK_SUBMITTER_NAME, "some");
        task.addParameter(PluginParameterKeys.MIME_TYPE, "image/tiff");
        task.addParameter(PluginParameterKeys.OUTPUT_MIME_TYPE, "image/jp2");
        String topologyName = "ic_topology";
        String user = VAN_PERSIE;
        grantUserToTopology(topologyName, user);
        login(user, VAN_PERSIE_PASSWORD);
        //then
        topologyTasksResource.submitTask(asyncResponse, task, topologyName, URI_INFO, AUTH_HEADER_VALUE);
    }

    @Test
    public void shouldThrowDpsTaskValidationExceptionOnSubmitTaskToIcTopologyWithMissingFileUrls() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {
        //when
        DpsTask task = new DpsTask("icTask");
        task.addParameter(PluginParameterKeys.TASK_SUBMITTER_NAME, "some");
        String topologyName = "ic_topology";
        String user = VAN_PERSIE;
        grantUserToTopology(topologyName, user);
        login(user, VAN_PERSIE_PASSWORD);
        try {
            //when
            topologyTasksResource.submitTask(asyncResponse, task, topologyName, URI_INFO, AUTH_HEADER_VALUE);
            fail();
        } catch (DpsTaskValidationException e) {
            //then
            assertThat(e.getMessage(), startsWith("Validation failed"));
        }
    }

    @Test
    public void shouldThrowDpsTaskValidationExceptionOnSubmitTaskToIcTopologyWithMissingTaskSubmitterName() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {
        //when
        IC_TASK = new DpsTask("icTask");
        IC_TASK.addDataEntry(DpsTask.FILE_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        IC_TASK.addParameter(PluginParameterKeys.MIME_TYPE, "mimeType");
        IC_TASK.addParameter(PluginParameterKeys.OUTPUT_MIME_TYPE, "mimeType");
        String topologyName = "ic_topology";
        String user = VAN_PERSIE;
        grantUserToTopology(topologyName, user);
        login(user, VAN_PERSIE_PASSWORD);
        try {
            //when
            topologyTasksResource.submitTask(asyncResponse, IC_TASK, topologyName, URI_INFO, AUTH_HEADER_VALUE);
            fail();
        } catch (DpsTaskValidationException e) {
            //then
            assertThat(e.getMessage(), is("Expected parameter does not exist in dpsTask. Parameter name: TASK_SUBMITTER_NAME"));
        }
    }

    private void grantUserToTopology(String topologyName, String user) throws AccessDeniedOrTopologyDoesNotExistException {
        login(ADMIN, ADMIN_PASSWORD);
        Mockito.when(topologyManager.containsTopology(topologyName)).thenReturn(true);
        Mockito.when(topologyManager.getNameToUserMap()).thenReturn(ImmutableMap.of(topologyName, "userName"));
        topologiesResource.grantPermissionsToTopology(user, topologyName);
        logoutEveryone();
    }

    @Test(expected = AccessDeniedException.class)
    public void shouldNotBeAbleToSubmitTaskToTopologyThatHasNotPermissionsTo() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(VAN_PERSIE, SAMPLE_TOPOLOGY_NAME);
        logoutEveryone();
        login(RONALDO, RONALD_PASSWORD);
        DpsTask sampleTask = new DpsTask();

        topologyTasksResource.submitTask(asyncResponse, sampleTask, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
    }

    // -- progress report tests --

    @Test
    public void shouldBeAbleToCheckProgressIfHeIsTheTaskOwner() throws AccessDeniedOrObjectDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {

        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(VAN_PERSIE, SAMPLE_TOPOLOGY_NAME);

        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
        topologyTasksResource.submitTask(asyncResponse, XSLT_TASK, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
        topologyTasksResource.getTaskProgress(SAMPLE_TOPOLOGY_NAME, "" + XSLT_TASK.getTaskId());
    }

    @Test(expected = AuthenticationCredentialsNotFoundException.class)
    public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToCheckProgress() throws AccessDeniedOrObjectDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException {

        topologyTasksResource.getTaskProgress(SAMPLE_TOPOLOGY_NAME, "" + XSLT_TASK.getTaskId());
    }


    @Test(expected = AccessDeniedException.class)
    public void vanPersieShouldNotBeAbleCheckProgressOfRonaldosTask() throws AccessDeniedOrObjectDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException {

        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);

        login(RONALDO, RONALD_PASSWORD);
        topologyTasksResource.submitTask(asyncResponse, XSLT_TASK, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
        topologyTasksResource.getTaskProgress(SAMPLE_TOPOLOGY_NAME, "" + XSLT_TASK.getTaskId());
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
            topologyTasksResource.submitTask(asyncResponse, XSLT_TASK, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
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
        Mockito.when(topologyManager.getNameToUserMap()).thenReturn(ImmutableMap.of(SAMPLE_TOPOLOGY_NAME, "userName"));
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);
        login(RONALDO, RONALD_PASSWORD);
        topologyTasksResource.submitTask(asyncResponse, XSLT_TASK, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
        //when
        try {
            topologyTasksResource.getTaskProgress(SAMPLE_TOPOLOGY_NAME, "" + XSLT_TASK.getTaskId());
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
        Mockito.when(topologyManager.getNameToUserMap()).thenReturn(ImmutableMap.of(SAMPLE_TOPOLOGY_NAME, "userName"));
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);
        login(RONALDO, RONALD_PASSWORD);
        topologyTasksResource.submitTask(asyncResponse, XSLT_TASK, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
        //when
        try {
            topologyTasksResource.killTask(SAMPLE_TOPOLOGY_NAME, "" + XSLT_TASK.getTaskId());
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
        Mockito.when(topologyManager.getNameToUserMap()).thenReturn(ImmutableMap.of(SAMPLE_TOPOLOGY_NAME, "userName"));
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);
        login(RONALDO, RONALD_PASSWORD);
        topologyTasksResource.submitTask(asyncResponse, XSLT_TASK, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
        //when
        try {
            topologyTasksResource.checkKillFlag(SAMPLE_TOPOLOGY_NAME, "" + XSLT_TASK.getTaskId());
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
        Mockito.when(topologyManager.getNameToUserMap()).thenReturn(ImmutableMap.of(SAMPLE_TOPOLOGY_NAME, "userName"));
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);
        login(RONALDO, RONALD_PASSWORD);
        topologyTasksResource.submitTask(asyncResponse, XSLT_TASK, SAMPLE_TOPOLOGY_NAME, URI_INFO, AUTH_HEADER_VALUE);
        //when
        try {
            topologyTasksResource.removeKillFlag(SAMPLE_TOPOLOGY_NAME, "" + XSLT_TASK.getTaskId());
            fail();
            //then
        } catch (AccessDeniedOrTopologyDoesNotExistException e) {
        }
    }


}

