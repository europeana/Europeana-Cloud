package eu.europeana.cloud.service.dps.controller;

import eu.europeana.cloud.common.model.dps.EngineTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.CreateDpsTaskRequest;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.DpsTaskValidationException;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounter;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import jakarta.validation.constraints.NotNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.security.authorization.AuthorizationDeniedException;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.context.web.WebAppConfiguration;

import java.io.IOException;
import java.util.Date;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@WebAppConfiguration
class DpsResourceAATest extends AbstractSecurityTest {

  /* Constants */
  public static final String PROVIDER_ID = "providerId";
  public static final String BATCH_ID = "batchId";

  private static final String VAN_PERSIE = "Robin_Van_Persie";
  private static final String VAN_PERSIE_PASSWORD = "Feyenoord";

  private static final String RONALDO = "Cristiano";
  private static final String RONALD_PASSWORD = "Ronaldo";
  private static final String ADMIN = "admin";
  private static final String ADMIN_PASSWORD = "admin";
  private static final long TASK_ID = 12345;
  private static final String SAMPLE_METIS_DATASET_ID = "ORG_DSID_DSNAME";

  private static final String SAMPLE_TOPOLOGY_NAME = "sampleTopology";
  private static final String XSLT_TOPOLOGY_NAME = "xslt_topology";
  private CreateDpsTaskRequest XSLT_TASK;
  private CreateDpsTaskRequest XSLT_TASK2;
  private CreateDpsTaskRequest XSLT_TASK_WITH_MALFORMED_URL;

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
  private CassandraTaskInfoDAO taskDAO;

  @Autowired
  private FilesCounterFactory filesCounterFactory;

  @Autowired
  private FilesCounter filesCounter;

  @Autowired
  public TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;

  private MockHttpServletRequest request;

  @BeforeEach
  void mockUp() throws Exception {
    XSLT_TASK = new CreateDpsTaskRequest();
    XSLT_TASK.setSource(prepateBatchInput());
    XSLT_TASK.addParameter(PluginParameterKeys.METIS_DATASET_ID, SAMPLE_METIS_DATASET_ID);
    XSLT_TASK.addParameter(PluginParameterKeys.XSLT_URL, "http://test.xslt");
    XSLT_TASK.setResultsProvider(PROVIDER_ID);

    XSLT_TASK2 = new CreateDpsTaskRequest();
    XSLT_TASK2.setSource(prepateBatchInput());
    XSLT_TASK2.addParameter(PluginParameterKeys.METIS_DATASET_ID, SAMPLE_METIS_DATASET_ID);
    XSLT_TASK2.setResultsProvider(PROVIDER_ID);

    XSLT_TASK_WITH_MALFORMED_URL = new CreateDpsTaskRequest();
    XSLT_TASK_WITH_MALFORMED_URL.setSource(prepateBatchInput());
    XSLT_TASK_WITH_MALFORMED_URL.addParameter(PluginParameterKeys.METIS_DATASET_ID, SAMPLE_METIS_DATASET_ID);
    XSLT_TASK_WITH_MALFORMED_URL.setResultsProvider(PROVIDER_ID);

    TaskInfo taskInfo = TaskInfo.builder()
                                .id(TASK_ID)
                                .topologyName(SAMPLE_TOPOLOGY_NAME)
            .state(EngineTaskState.PROCESSED)
                                .stateDescription("")
            .expectedRecords(100)
            .processedRecords(100)
            .failRecords(0)
                                .sentTimestamp(new Date())
                                .startTimestamp(new Date())
                                .finishTimestamp(new Date())
                                .build();

    when(taskDAO.findById(anyLong())).thenReturn(Optional.empty());
    Mockito.doReturn(taskInfo).when(reportService).getTaskProgress(Mockito.anyLong());
    when(topologyManager.containsTopology(SAMPLE_TOPOLOGY_NAME)).thenReturn(true);
    when(filesCounterFactory.createFilesCounter(any(DpsTask.class), anyString())).thenReturn(filesCounter);
    request = new MockHttpServletRequest();

  }

  /*
      Task Submission tests
   */
  @Test
  void shouldThrowExceptionWhenNonAuthenticatedUserTriesToSubmitTask() {

    CreateDpsTaskRequest t = new CreateDpsTaskRequest();
    String topology = "xsltTopology";

    assertThrows(AuthenticationCredentialsNotFoundException.class, () -> topologyTasksResource.createTask(request, t, topology));
  }

  @Test
  void shouldBeAbleToSubmitTaskToTopologyThatHasPermissionsTo()
          throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, IOException {
    login(ADMIN, ADMIN_PASSWORD);
    when(topologyManager.containsTopology(XSLT_TOPOLOGY_NAME)).thenReturn(true);
    topologiesResource.grantPermissionsToTopology(VAN_PERSIE, XSLT_TOPOLOGY_NAME);
    logoutEveryone();
    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
    CreateDpsTaskRequest task = new CreateDpsTaskRequest();
    task.setSource(prepateBatchInput());
    task.addParameter(PluginParameterKeys.MIME_TYPE, "image/tiff");
    task.addParameter(PluginParameterKeys.OUTPUT_MIME_TYPE, "image/jp2");

    task.addParameter(PluginParameterKeys.XSLT_URL, "http://test.xslt");
    task.setResultsProvider(PROVIDER_ID);

    topologyTasksResource.createTask(request, task, XSLT_TOPOLOGY_NAME);
  }

  @Test
  void shouldBeAbleToSubmitTaskToXsltTopology()
          throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, IOException {
    //when
    CreateDpsTaskRequest task = new CreateDpsTaskRequest();
    task.setSource(prepateBatchInput());
    task.addParameter(PluginParameterKeys.XSLT_URL, "http://test.xslt");
    task.addParameter(PluginParameterKeys.METIS_DATASET_ID, SAMPLE_METIS_DATASET_ID);

    task.addParameter(PluginParameterKeys.XSLT_URL, "http://test.xslt");
    task.setResultsProvider(PROVIDER_ID);

    String topologyName = "xslt_topology";
    String user = VAN_PERSIE;
    grantUserToTopology(topologyName, user);
    login(user, VAN_PERSIE_PASSWORD);
    //then
    topologyTasksResource.createTask(request, task, topologyName);
  }

  @Test
  void shouldThrowDpsTaskValidationExceptionOnSubmitTaskToXsltTopologyWithMissingInput()
          throws AccessDeniedOrTopologyDoesNotExistException, IOException {
    //when
    CreateDpsTaskRequest task = new CreateDpsTaskRequest();
    task.addParameter(PluginParameterKeys.XSLT_URL, "http://test.xslt");
    task.addParameter(PluginParameterKeys.METIS_DATASET_ID, SAMPLE_METIS_DATASET_ID);
    String topologyName = "xslt_topology";
    String user = VAN_PERSIE;
    grantUserToTopology(topologyName, user);
    login(user, VAN_PERSIE_PASSWORD);
    try {
      //when
      topologyTasksResource.createTask(request, task, topologyName);
      fail();
    } catch (DpsTaskValidationException e) {
      //then
      assertThat(e.getMessage(), containsString("source"));
    }
  }

  @Test
  void shouldThrowDpsTaskValidationExceptionOnSubmitTaskToXsltTopologyWithMissingXsltUrl()
          throws AccessDeniedOrTopologyDoesNotExistException, IOException {
    //when
    CreateDpsTaskRequest task = new CreateDpsTaskRequest();
    task.setSource(prepateBatchInput());
    task.addParameter(PluginParameterKeys.METIS_DATASET_ID, SAMPLE_METIS_DATASET_ID);
    String topologyName = "xslt_topology";
    String user = VAN_PERSIE;
    grantUserToTopology(topologyName, user);
    login(user, VAN_PERSIE_PASSWORD);
    try {
      //when
      topologyTasksResource.createTask(request, task, topologyName);
      fail();
    } catch (DpsTaskValidationException e) {
      //then
      assertThat(e.getMessage(), is("Expected parameter does not exist in dpsTask. Parameter name: XSLT_URL"));
    }
  }

  private void grantUserToTopology(String topologyName, String user) throws AccessDeniedOrTopologyDoesNotExistException {
    login(ADMIN, ADMIN_PASSWORD);
    when(topologyManager.containsTopology(topologyName)).thenReturn(true);
    topologiesResource.grantPermissionsToTopology(user, topologyName);
    logoutEveryone();
  }

  @Test
  void shouldNotBeAbleToSubmitTaskToTopologyThatHasNotPermissionsTo()
          throws AccessDeniedOrTopologyDoesNotExistException {
    login(ADMIN, ADMIN_PASSWORD);
    topologiesResource.grantPermissionsToTopology(VAN_PERSIE, SAMPLE_TOPOLOGY_NAME);
    logoutEveryone();
    login(RONALDO, RONALD_PASSWORD);
    CreateDpsTaskRequest sampleTask = new CreateDpsTaskRequest();
    Assertions.assertThrows(AuthorizationDeniedException.class,
            () -> topologyTasksResource.createTask(request, sampleTask, SAMPLE_TOPOLOGY_NAME));

  }

  // -- progress report tests --

  @Test
  void shouldBeAbleToCheckProgressIfHeIsTheTaskOwner()
          throws AccessDeniedOrObjectDoesNotExistException, AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, IOException {

    when(topologyManager.containsTopology(XSLT_TOPOLOGY_NAME)).thenReturn(true);
    login(ADMIN, ADMIN_PASSWORD);
    topologiesResource.grantPermissionsToTopology(VAN_PERSIE, XSLT_TOPOLOGY_NAME);

    login(VAN_PERSIE, VAN_PERSIE_PASSWORD);
    long taskId = topologyTasksResource.createTask(request, XSLT_TASK, XSLT_TOPOLOGY_NAME).getBody().getTaskId();
    topologyTasksResource.getTaskProgress(XSLT_TOPOLOGY_NAME, taskId);
  }

  @Test
  void shouldThrowExceptionWhenNonAuthenticatedUserTriesToCheckProgress() {
    Long taskId = 213243124L;
    Assertions.assertThrows(AuthenticationCredentialsNotFoundException.class,
            () -> topologyTasksResource.getTaskProgress(SAMPLE_TOPOLOGY_NAME, taskId));
  }


  @Test
  void vanPersieShouldNotBeAbleCheckProgressOfRonaldosTask() throws
          AccessDeniedOrTopologyDoesNotExistException {

    login(ADMIN, ADMIN_PASSWORD);
    topologiesResource.grantPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);

    login(RONALDO, RONALD_PASSWORD);
    Assertions.assertThrows(AuthorizationDeniedException.class,
            () -> topologyTasksResource.createTask(request, XSLT_TASK, XSLT_TOPOLOGY_NAME));
  }

  @Test
  void vanPersieShouldNotBeAbleGrantPermissionsToNotDefinedTopology() {
    final String FAIL_TOPOLOGY_NAME = "failTopology";
    //given
    login(ADMIN, ADMIN_PASSWORD);

    //when
    Assertions.assertThrows(AccessDeniedOrTopologyDoesNotExistException.class,
            () -> topologiesResource.grantPermissionsToTopology(RONALDO, FAIL_TOPOLOGY_NAME));
    //then - intentionally empty
  }

  @Test
  void vanPersieShouldNotBeAbleSubmitTaskToNotDefinedTopology()
          throws AccessDeniedOrTopologyDoesNotExistException {
    //given

    reset(topologyManager);
    when(topologyManager.containsTopology(SAMPLE_TOPOLOGY_NAME)).thenReturn(true, false);

    login(ADMIN, ADMIN_PASSWORD);
    topologiesResource.grantPermissionsToTopology(RONALDO, SAMPLE_TOPOLOGY_NAME);
    login(RONALDO, RONALD_PASSWORD);
    assertThrows(AccessDeniedOrTopologyDoesNotExistException.class, () -> topologyTasksResource.createTask(request, XSLT_TASK, SAMPLE_TOPOLOGY_NAME));

  }

  @Test
  void vanPersieShouldNotBeAbleGetTaskProgressToNotDefinedTopology() throws
          AccessDeniedOrTopologyDoesNotExistException,
          DpsTaskValidationException, IOException {
    //given

    reset(topologyManager);
    when(topologyManager.containsTopology(XSLT_TOPOLOGY_NAME)).thenReturn(true, true, false);
    login(ADMIN, ADMIN_PASSWORD);
    topologiesResource.grantPermissionsToTopology(RONALDO, XSLT_TOPOLOGY_NAME);
    login(RONALDO, RONALD_PASSWORD);
    long taskId = topologyTasksResource.createTask(request, XSLT_TASK, XSLT_TOPOLOGY_NAME).getBody().getTaskId();
    assertThrows(AccessDeniedOrTopologyDoesNotExistException.class, () -> topologyTasksResource.getTaskProgress(XSLT_TOPOLOGY_NAME, taskId));

  }


  @Test
  void UserShouldNotBeAbleKillTaskToNotDefinedTopology()
          throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, IOException {
    //given
    reset(topologyManager);
    when(topologyManager.containsTopology(XSLT_TOPOLOGY_NAME)).thenReturn(true, true, false);
    login(ADMIN, ADMIN_PASSWORD);
    topologiesResource.grantPermissionsToTopology(RONALDO, XSLT_TOPOLOGY_NAME);
    login(RONALDO, RONALD_PASSWORD);
    long taskId = topologyTasksResource.createTask(request, XSLT_TASK, XSLT_TOPOLOGY_NAME).getBody().getTaskId();

    assertThrows(AccessDeniedOrTopologyDoesNotExistException.class, () -> topologyTasksResource.killTask(XSLT_TOPOLOGY_NAME, taskId, "Dropped by the user"));
  }

  @Test
  void UserShouldNotBeAbleKillTaskHeDidNotSend()
          throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, IOException {
    //given
    reset(topologyManager);
    when(topologyManager.containsTopology(XSLT_TOPOLOGY_NAME)).thenReturn(true, true, true);
    login(ADMIN, ADMIN_PASSWORD);
    topologiesResource.grantPermissionsToTopology(ADMIN, XSLT_TOPOLOGY_NAME);
    long taskId =topologyTasksResource.createTask(request, XSLT_TASK, XSLT_TOPOLOGY_NAME).getBody().getTaskId();
    login(RONALDO, RONALD_PASSWORD);
    assertThrows(AccessDeniedException.class, () -> topologyTasksResource.killTask(XSLT_TOPOLOGY_NAME, taskId, "Dropped by the user"));
  }

  @Test
  void UserShouldBeAbleKillTaskHeSent()
          throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, IOException, AccessDeniedOrObjectDoesNotExistException {
    //given
    reset(topologyManager);
    when(topologyManager.containsTopology(XSLT_TOPOLOGY_NAME)).thenReturn(true, true, true);
    login(ADMIN, ADMIN_PASSWORD);
    topologiesResource.grantPermissionsToTopology(ADMIN, XSLT_TOPOLOGY_NAME);
    long taskId = topologyTasksResource.createTask(request, XSLT_TASK, XSLT_TOPOLOGY_NAME).getBody().getTaskId();
    ResponseEntity<String> response = topologyTasksResource.killTask(XSLT_TOPOLOGY_NAME, taskId,
            "Dropped by the user");
    assertNotNull(response);
    assertEquals(200, response.getStatusCode().value());
  }


  private static BatchInfo prepateBatchInput() {
    return BatchInfo.builder()
                    .providerId(PROVIDER_ID)
                    .batchId(BATCH_ID)
                    .build();
  }
}

