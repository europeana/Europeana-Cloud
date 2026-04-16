package eu.europeana.cloud.service.dps.controller;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.model.dps.EngineTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.DatasetRevisionInfo.DatasetRevisionInfoBuilder;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(SpringExtension.class)
@WebAppConfiguration
class DpsResourceAATest extends AbstractSecurityTest {

  /* Constants */
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
  private DpsTask XSLT_TASK;
  private DpsTask XSLT_TASK2;
  private DpsTask XSLT_TASK_WITH_MALFORMED_URL;

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
    XSLT_TASK = new DpsTask("xsltTask");
    XSLT_TASK.setInput(new FilesUrls(
            "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
    XSLT_TASK.addParameter(PluginParameterKeys.METIS_DATASET_ID, SAMPLE_METIS_DATASET_ID);
    XSLT_TASK.addParameter(PluginParameterKeys.XSLT_URL, "http://test.xslt");
    XSLT_TASK.setOutput(prepareCompleteDatasetRevisionInfo().build());

    XSLT_TASK2 = new DpsTask("xsltTask");
    XSLT_TASK2.setInput(new FilesUrls(
        "http://127.0.0.1:8080/mcs/records/sampleId/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
    XSLT_TASK2.addParameter(PluginParameterKeys.METIS_DATASET_ID, SAMPLE_METIS_DATASET_ID);
    XSLT_TASK2.setOutput(prepareCompleteDatasetRevisionInfo().build());

    XSLT_TASK_WITH_MALFORMED_URL = new DpsTask("taskWithMalformedUrl");
    XSLT_TASK_WITH_MALFORMED_URL.setInput(new FilesUrls(
        "httpz://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
    XSLT_TASK_WITH_MALFORMED_URL.addParameter(PluginParameterKeys.METIS_DATASET_ID, SAMPLE_METIS_DATASET_ID);
    XSLT_TASK_WITH_MALFORMED_URL.setOutput(prepareCompleteDatasetRevisionInfo().build());

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

    DpsTask t = new DpsTask("xsltTask");
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
    DpsTask task = new DpsTask();
    task.setInput(new FilesUrls(
            "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
    task.addParameter(PluginParameterKeys.MIME_TYPE, "image/tiff");
    task.addParameter(PluginParameterKeys.OUTPUT_MIME_TYPE, "image/jp2");

    task.addParameter(PluginParameterKeys.XSLT_URL, "http://test.xslt");
    task.setOutput(prepareCompleteDatasetRevisionInfo().build());

    topologyTasksResource.createTask(request, task, XSLT_TOPOLOGY_NAME);
  }

  @Test
  void shouldBeAbleToSubmitTaskToXsltTopology()
          throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, IOException {
    //when
    DpsTask task = new DpsTask("xsltTask");
    task.setInput(new FilesUrls(
            "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
    task.addParameter(PluginParameterKeys.XSLT_URL, "http://test.xslt");
    task.addParameter(PluginParameterKeys.METIS_DATASET_ID, SAMPLE_METIS_DATASET_ID);

    task.addParameter(PluginParameterKeys.XSLT_URL, "http://test.xslt");
    task.setOutput(prepareCompleteDatasetRevisionInfo().build());

    String topologyName = "xslt_topology";
    String user = VAN_PERSIE;
    grantUserToTopology(topologyName, user);
    login(user, VAN_PERSIE_PASSWORD);
    //then
    topologyTasksResource.createTask(request, task, topologyName);
  }

  @Test
  void shouldThrowDpsTaskValidationExceptionOnSubmitTaskToXsltTopologyWithMissingFileUrls()
          throws AccessDeniedOrTopologyDoesNotExistException, IOException {
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
      topologyTasksResource.createTask(request, task, topologyName);
      fail();
    } catch (DpsTaskValidationException e) {
      //then
      assertThat(e.getMessage(), startsWith("Expected parameter"));
    }
  }

  @Test
  void shouldThrowDpsTaskValidationExceptionOnSubmitTaskToXsltTopologyWithMissingXsltUrl()
          throws AccessDeniedOrTopologyDoesNotExistException, IOException {
    //when
    DpsTask task = new DpsTask("xsltTask");
    task.setInput(new FilesUrls(
            "http://127.0.0.1:8080/mcs/records/FUWQ4WMUGIGEHVA3X7FY5PA3DR5Q4B2C4TWKNILLS6EM4SJNTVEQ/representations/TIFF/versions/86318b00-6377-11e5-a1c6-90e6ba2d09ef/files/sampleFileName.txt"));
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
    DpsTask sampleTask = new DpsTask();
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
    topologyTasksResource.createTask(request, XSLT_TASK, XSLT_TOPOLOGY_NAME);
    topologyTasksResource.getTaskProgress(XSLT_TOPOLOGY_NAME, XSLT_TASK.getTaskId());
  }

  @Test
  void shouldThrowExceptionWhenNonAuthenticatedUserTriesToCheckProgress() {
    Long taskId = XSLT_TASK.getTaskId();
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
    topologyTasksResource.createTask(request, XSLT_TASK, XSLT_TOPOLOGY_NAME);
    assertThrows(AccessDeniedOrTopologyDoesNotExistException.class, () -> topologyTasksResource.getTaskProgress(XSLT_TOPOLOGY_NAME, XSLT_TASK.getTaskId()));

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
    topologyTasksResource.createTask(request, XSLT_TASK, XSLT_TOPOLOGY_NAME);

    assertThrows(AccessDeniedOrTopologyDoesNotExistException.class, () -> topologyTasksResource.killTask(XSLT_TOPOLOGY_NAME, XSLT_TASK.getTaskId(), "Dropped by the user"));
  }

  @Test
  void UserShouldNotBeAbleKillTaskHeDidNotSend()
          throws AccessDeniedOrTopologyDoesNotExistException, DpsTaskValidationException, IOException {
    //given
    reset(topologyManager);
    when(topologyManager.containsTopology(XSLT_TOPOLOGY_NAME)).thenReturn(true, true, true);
    login(ADMIN, ADMIN_PASSWORD);
    topologiesResource.grantPermissionsToTopology(ADMIN, XSLT_TOPOLOGY_NAME);
    topologyTasksResource.createTask(request, XSLT_TASK, XSLT_TOPOLOGY_NAME);
    login(RONALDO, RONALD_PASSWORD);
    long taskId = XSLT_TASK.getTaskId();
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
    topologyTasksResource.createTask(request, XSLT_TASK, XSLT_TOPOLOGY_NAME);
    ResponseEntity<String> response = topologyTasksResource.killTask(XSLT_TOPOLOGY_NAME, XSLT_TASK.getTaskId(),
            "Dropped by the user");
    assertNotNull(response);
    assertEquals(200, response.getStatusCode().value());
  }

  public static DatasetRevisionInfoBuilder prepareCompleteDatasetRevisionInfo() {
    return DatasetRevisionInfo.builder().providerId("providerId").datasetId("datasetId").
                              representationName("representationName")
                              .revision(new Revision("sampleRevisionName", "sampleRevisionProvider", new Date()));
  }
}

