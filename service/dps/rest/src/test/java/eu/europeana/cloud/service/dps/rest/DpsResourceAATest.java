package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounter;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static eu.europeana.cloud.service.dps.InputDataType.FILE_URLS;
import static junit.framework.Assert.assertEquals;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

@RunWith(SpringRunner.class)
@WebAppConfiguration
public class DpsResourceAATest extends AbstractSecurityTest {
    /* Constants */
    private final static String VAN_PERSIE = "Robin_Van_Persie";
    private final static String VAN_PERSIE_PASSWORD = "Feyenoord";

    private final static String RONALDO = "Cristiano";
    private final static String RONALD_PASSWORD = "Ronaldo";

    private final static String ADMIN = "admin";
    private final static String ADMIN_PASSWORD = "admin";
    private final static long TASK_ID = 12345;
    private final static String SAMPLE_METIS_DATASET_ID = "ORG_DSID_DSNAME";

    private final static String SAMPLE_TOPOLOGY_NAME = "sampleTopology";
    private DpsTask XSLT_TASK;
    private DpsTask XSLT_TASK2;
    private DpsTask XSLT_TASK_WITH_MALFORMED_URL;

    private final static String AUTH_HEADER_VALUE = "header_value";

    /* Beans and mocked beans */
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

    @Autowired
    private FilesCounterFactory filesCounterFactory;

    @Autowired
    private FilesCounter filesCounter;

    private MockHttpServletRequest request;

    @Before
    public void mockUp() throws Exception {
        XSLT_TASK = new DpsTask("xsltTask");
        XSLT_TASK.addDataEntry(FILE_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        XSLT_TASK.addParameter(PluginParameterKeys.METIS_DATASET_ID, SAMPLE_METIS_DATASET_ID);

        XSLT_TASK2 = new DpsTask("xsltTask");
        XSLT_TASK2.addDataEntry(FILE_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/records/sampleId/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        XSLT_TASK2.addParameter(PluginParameterKeys.METIS_DATASET_ID, SAMPLE_METIS_DATASET_ID);

        XSLT_TASK_WITH_MALFORMED_URL = new DpsTask("taskWithMalformedUrl");
        XSLT_TASK_WITH_MALFORMED_URL.addDataEntry(FILE_URLS, Arrays.asList("httpz://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        XSLT_TASK_WITH_MALFORMED_URL.addParameter(PluginParameterKeys.METIS_DATASET_ID, SAMPLE_METIS_DATASET_ID);

        TaskInfo taskInfo = new TaskInfo(TASK_ID, SAMPLE_TOPOLOGY_NAME, TaskState.PROCESSED, "", 100, 100, 0, 0, new Date(), new Date(), new Date());
        when(taskDAO.findById(anyLong())).thenReturn(Optional.empty());
        Mockito.doReturn(taskInfo).when(reportService).getTaskProgress(Mockito.anyString());
        when(topologyManager.containsTopology(SAMPLE_TOPOLOGY_NAME)).thenReturn(true);
        doNothing().when(recordServiceClient).useAuthorizationHeader(anyString());
        when(filesCounterFactory.createFilesCounter(any(DpsTask.class), anyString())).thenReturn(filesCounter);
        request = new MockHttpServletRequest();

    }

    /*
        Task Submission tests
     */
    @Test(expected = AuthenticationCredentialsNotFoundException.class)
    public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToSubmitTask() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException, IOException, ExecutionException, InterruptedException {

        DpsTask t = new DpsTask("xsltTask");
        String topology = "xsltTopology";

        topologyTasksResource.submitTask(request,  t, topology,  AUTH_HEADER_VALUE);
    }

    @Test
    public void shouldBeAbleToSubmitTaskToTopologyThatHasPermissionsTo() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException, IOException, ExecutionException, InterruptedException {
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(VAN_PERSIE, SAMPLE_TOPOLOGY_NAME);
        logoutEveryone();
        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
        DpsTask task = new DpsTask();
        task.addDataEntry(FILE_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        task.addParameter(PluginParameterKeys.MIME_TYPE, "image/tiff");
        task.addParameter(PluginParameterKeys.OUTPUT_MIME_TYPE, "image/jp2");
        topologyTasksResource.submitTask(request, task, SAMPLE_TOPOLOGY_NAME, AUTH_HEADER_VALUE);
    }

    @Test
    public void shouldBeAbleToSubmitTaskToXsltTopology() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException, IOException, ExecutionException, InterruptedException {
        //when
        DpsTask task = new DpsTask("xsltTask");
        task.addDataEntry(FILE_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        task.addParameter(PluginParameterKeys.XSLT_URL, "http://test.xslt");
        task.addParameter(PluginParameterKeys.METIS_DATASET_ID, SAMPLE_METIS_DATASET_ID);
        String topologyName = "xslt_topology";
        String user = VAN_PERSIE;
        grantUserToTopology(topologyName, user);
        login(user, VAN_PERSIE_PASSWORD);
        //then
        topologyTasksResource.submitTask(request, task, topologyName, AUTH_HEADER_VALUE);
    }

    @Test
    public void shouldThrowDpsTaskValidationExceptionOnSubmitTaskToXsltTopologyWithMissingFileUrls() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException, IOException, ExecutionException, InterruptedException {
        //when
        DpsTask task = new DpsTask("xsltTask");
        task.addParameter(PluginParameterKeys.XSLT_URL, "http://test.xslt");
        task.addParameter(PluginParameterKeys.METIS_DATASET_ID, SAMPLE_METIS_DATASET_ID);
        String topologyName = "xslt_topology";
        String user = VAN_PERSIE;
        grantUserToTopology(topologyName, user);
        login(user, VAN_PERSIE_PASSWORD);
        try {
            //when
            topologyTasksResource.submitTask(request, task, topologyName, AUTH_HEADER_VALUE);
            fail();
        } catch (DpsTaskValidationException e) {
            //then
            assertThat(e.getMessage(), startsWith("Validation failed"));
        }
    }

    @Test
    public void shouldThrowDpsTaskValidationExceptionOnSubmitTaskToXsltTopologyWithMissingXsltUrl() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException, IOException, ExecutionException, InterruptedException {
        //when
        DpsTask task = new DpsTask("xsltTask");
        task.addDataEntry(FILE_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        task.addParameter(PluginParameterKeys.METIS_DATASET_ID, SAMPLE_METIS_DATASET_ID);
        String topologyName = "xslt_topology";
        String user = VAN_PERSIE;
        grantUserToTopology(topologyName, user);
        login(user, VAN_PERSIE_PASSWORD);
        try {
            //when
            topologyTasksResource.submitTask(request, task, topologyName, AUTH_HEADER_VALUE);
            fail();
        } catch (DpsTaskValidationException e) {
            //then
            assertThat(e.getMessage(), is("Expected parameter does not exist in dpsTask. Parameter name: XSLT_URL"));
        }
    }


    @Test
    public void shouldBeAbleToSubmitTaskToIcTopology() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException, IOException, ExecutionException, InterruptedException {
        //when
        DpsTask task = new DpsTask("icTask");
        task.addDataEntry(FILE_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        task.addParameter(PluginParameterKeys.MIME_TYPE, "image/tiff");
        task.addParameter(PluginParameterKeys.OUTPUT_MIME_TYPE, "image/jp2");
        String topologyName = "ic_topology";
        String user = VAN_PERSIE;
        grantUserToTopology(topologyName, user);
        login(user, VAN_PERSIE_PASSWORD);
        //then
        topologyTasksResource.submitTask(request, task, topologyName, AUTH_HEADER_VALUE);
    }

    @Test
    public void shouldNotBeAbleToSubmitTaskToIcTopologyWithUnacceptedOutputMimeType() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException, IOException, ExecutionException, InterruptedException {
        //when
        DpsTask task = new DpsTask("icTask");
        task.addDataEntry(FILE_URLS, Arrays.asList("http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
        task.addParameter(PluginParameterKeys.MIME_TYPE, "image/tiff");
        task.addParameter(PluginParameterKeys.OUTPUT_MIME_TYPE, "undefined");
        String topologyName = "ic_topology";
        String user = VAN_PERSIE;
        grantUserToTopology(topologyName, user);
        login(user, VAN_PERSIE_PASSWORD);
        //then
        try {
            topologyTasksResource.submitTask(request, task, topologyName, AUTH_HEADER_VALUE);
            fail();
        } catch (DpsTaskValidationException e) {
            assertThat(e.getMessage(), is("Parameter does not meet constraints. Parameter name: OUTPUT_MIME_TYPE"));
        }
    }

    @Test
    public void shouldThrowDpsTaskValidationExceptionOnSubmitTaskToIcTopologyWithMissingFileUrls() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException, IOException, ExecutionException, InterruptedException {
        //when
        DpsTask task = new DpsTask("icTask");
        String topologyName = "ic_topology";
        String user = VAN_PERSIE;
        grantUserToTopology(topologyName, user);
        login(user, VAN_PERSIE_PASSWORD);
        try {
            //when
            topologyTasksResource.submitTask(request, task, topologyName, AUTH_HEADER_VALUE);
            fail();
        } catch (DpsTaskValidationException e) {
            //then
            assertThat(e.getMessage(), startsWith("Validation failed"));
        }
    }


    private void grantUserToTopology(String topologyName, String user) throws AccessDeniedOrTopologyDoesNotExistException {
        login(ADMIN, ADMIN_PASSWORD);
        when(topologyManager.containsTopology(topologyName)).thenReturn(true);
        topologiesResource.grantPermissionsToTopology(user, topologyName);
        logoutEveryone();
    }

    @Test(expected = AccessDeniedException.class)
    public void shouldNotBeAbleToSubmitTaskToTopologyThatHasNotPermissionsTo() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException, IOException, ExecutionException, InterruptedException {
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(VAN_PERSIE, SAMPLE_TOPOLOGY_NAME);
        logoutEveryone();
        login(RONALDO, RONALD_PASSWORD);
        DpsTask sampleTask = new DpsTask();
        topologyTasksResource.submitTask(request, sampleTask, SAMPLE_TOPOLOGY_NAME, AUTH_HEADER_VALUE);
    }

    // -- progress report tests --

    @Test
    public void shouldBeAbleToCheckProgressIfHeIsTheTaskOwner() throws AccessDeniedOrObjectDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException, IOException, ExecutionException, InterruptedException {

        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(VAN_PERSIE, SAMPLE_TOPOLOGY_NAME);

        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
        submitTaskAndWait(XSLT_TASK, SAMPLE_TOPOLOGY_NAME, AUTH_HEADER_VALUE);
        topologyTasksResource.getTaskProgress(SAMPLE_TOPOLOGY_NAME, "" + XSLT_TASK.getTaskId());
    }

    @Test(expected = AuthenticationCredentialsNotFoundException.class)
    public void shouldThrowExceptionWhenNonAuthenticatedUserTriesToCheckProgress() throws
            AccessDeniedOrObjectDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException {

        topologyTasksResource.getTaskProgress(SAMPLE_TOPOLOGY_NAME, "" + XSLT_TASK.getTaskId());
    }


    @Test(expected = AccessDeniedException.class)
    public void vanPersieShouldNotBeAbleCheckProgressOfRonaldosTask() throws AccessDeniedOrObjectDoesNotExistException,
            AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, IOException, ExecutionException,
            InterruptedException {

        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);

        login(RONALDO, RONALD_PASSWORD);
        submitTaskAndWait(XSLT_TASK, SAMPLE_TOPOLOGY_NAME, AUTH_HEADER_VALUE);
        login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
        topologyTasksResource.getTaskProgress(SAMPLE_TOPOLOGY_NAME, "" + XSLT_TASK.getTaskId());
    }

    @Test(expected = AccessDeniedOrTopologyDoesNotExistException.class)
    public void vanPersieShouldNotBeAbleGrantPermissionsToNotDefinedTopology() throws
            AccessDeniedOrTopologyDoesNotExistException {
        final String FAIL_TOPOLOGY_NAME = "failTopology";
        //given
        login(ADMIN, ADMIN_PASSWORD);

        //when
        topologiesResource.grantPermissionsToTopology(RONALDO, FAIL_TOPOLOGY_NAME);

        //then - intentionally empty
    }

    @Test
    public void vanPersieShouldNotBeAbleSubmitTaskToNotDefinedTopology() throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, TaskSubmissionException, IOException, ExecutionException, InterruptedException {
        //given

        reset(topologyManager);
        when(topologyManager.containsTopology(SAMPLE_TOPOLOGY_NAME)).thenReturn(true, false);

        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);
        login(RONALDO, RONALD_PASSWORD);
        //when
        try {
            topologyTasksResource.submitTask(request, XSLT_TASK, SAMPLE_TOPOLOGY_NAME, AUTH_HEADER_VALUE);
            fail();
            //then
        } catch (AccessDeniedOrTopologyDoesNotExistException e) {
        }
    }

    @Test
    public void vanPersieShouldNotBeAbleGetTaskProgressToNotDefinedTopology() throws
            AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException,
            DpsTaskValidationException, IOException, ExecutionException, InterruptedException {
        //given

        reset(topologyManager);
        when(topologyManager.containsTopology(SAMPLE_TOPOLOGY_NAME)).thenReturn(true, true, false);
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);
        login(RONALDO, RONALD_PASSWORD);
        submitTaskAndWait( XSLT_TASK, SAMPLE_TOPOLOGY_NAME, AUTH_HEADER_VALUE);
        //when
        try {
            topologyTasksResource.getTaskProgress(SAMPLE_TOPOLOGY_NAME, "" + XSLT_TASK.getTaskId());
            fail();
            //then
        } catch (AccessDeniedOrTopologyDoesNotExistException e) {
        }
    }


    @Test
    public void UserShouldNotBeAbleKillTaskToNotDefinedTopology() throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException, DpsTaskValidationException, TaskSubmissionException, IOException, ExecutionException, InterruptedException {
        //given
        reset(topologyManager);
        when(topologyManager.containsTopology(SAMPLE_TOPOLOGY_NAME)).thenReturn(true, true, false);
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);
        login(RONALDO, RONALD_PASSWORD);
        submitTaskAndWait( XSLT_TASK, SAMPLE_TOPOLOGY_NAME, AUTH_HEADER_VALUE);
        //when
        try {
            topologyTasksResource.killTask(SAMPLE_TOPOLOGY_NAME, "" + XSLT_TASK.getTaskId(),"Dropped by the user");
            fail();
            //then
        } catch (AccessDeniedOrTopologyDoesNotExistException e) {
        }
    }

    @Test
    public void UserShouldNotBeAbleKillTaskHeDidNotSend() throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException, DpsTaskValidationException, TaskSubmissionException, IOException, ExecutionException, InterruptedException {
        //given
        reset(topologyManager);
        when(topologyManager.containsTopology(SAMPLE_TOPOLOGY_NAME)).thenReturn(true, true, true);
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(ADMIN, SAMPLE_TOPOLOGY_NAME);
        submitTaskAndWait(XSLT_TASK, SAMPLE_TOPOLOGY_NAME, AUTH_HEADER_VALUE);
        login(RONALDO, RONALD_PASSWORD);

        //when
        try {
            topologyTasksResource.killTask(SAMPLE_TOPOLOGY_NAME, "" + XSLT_TASK.getTaskId(),"Dropped by the user");
            fail();
            //then
        } catch (AccessDeniedException e) {
        }
    }

    @Test
    public void UserShouldBeAbleKillTaskHeSent() throws AccessDeniedOrTopologyDoesNotExistException, AccessDeniedOrObjectDoesNotExistException, DpsTaskValidationException, TaskSubmissionException, IOException, ExecutionException, InterruptedException {
        //given
        reset(topologyManager);
        when(topologyManager.containsTopology(SAMPLE_TOPOLOGY_NAME)).thenReturn(true, true, true);
        login(ADMIN, ADMIN_PASSWORD);
        topologiesResource.grantPermissionsToTopology(ADMIN, SAMPLE_TOPOLOGY_NAME);
        submitTaskAndWait(XSLT_TASK, SAMPLE_TOPOLOGY_NAME, AUTH_HEADER_VALUE);
        //when
        try {
            ResponseEntity<String> response = topologyTasksResource.killTask(SAMPLE_TOPOLOGY_NAME, "" + XSLT_TASK.getTaskId(),"Dropped by the user");
            assertNotNull(response);
            assertEquals(200, response.getStatusCodeValue());
        } catch (Exception e) {
            fail();
        }
    }

    void submitTaskAndWait(DpsTask dpsTask, String topologyName, String authHeader)
            throws DpsTaskValidationException, AccessDeniedOrTopologyDoesNotExistException,
            IOException, ExecutionException, InterruptedException {
        topologyTasksResource.submitTask(request, dpsTask, topologyName, authHeader);
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

}

