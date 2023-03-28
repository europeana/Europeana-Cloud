package eu.europeana.cloud.service.dps.storm;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.common.model.dps.TaskErrorInfo;
import eu.europeana.cloud.common.model.dps.TaskErrorsInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.NotificationsDAO;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.notification.handler.NotificationTupleHandler;
import eu.europeana.cloud.service.dps.storm.service.TaskExecutionReportServiceImpl;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
import eu.europeana.cloud.service.dps.storm.utils.StormTaskTupleHelper;
import eu.europeana.cloud.test.CassandraTestInstance;
import eu.europeana.enrichment.rest.client.report.Report;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.storm.Config;
import org.apache.storm.task.GeneralTopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.awaitility.Durations;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class NotificationBoltTest extends CassandraTestBase {

  public static final String USER_NAME = "";
  public static final String PASSWORD = "";
  public static final String RESOURCE_1 = "resource1";
  public static final String RESOURCE_2 = "resource2";
  public static final String RESOURCE_3 = "resource3";
  public static final String RESOURCE_4 = "resource4";
  public static final String RESOURCE_5 = "resource5";
  public static final String RESOURCE_6 = "resource6";
  private static final String TOPOLOGY_NAME = "test_topology";
  public static final int TASK_ID = 1;
  private OutputCollector collector;
  private NotificationBolt testedBolt;
  private CassandraTaskInfoDAO taskInfoDAO;
  private CassandraTaskErrorsDAO taskErrorsDAO;
  private TaskExecutionReportServiceImpl reportService;
  private NotificationsDAO subtaskDAO;
  private ProcessedRecordsDAO processedRecordsDAO;
  private int resourceCounter = 0;

  @Before
  public void setUp() throws Exception {
    collector = Mockito.mock(OutputCollector.class);
    createBolt();
    CassandraConnectionProvider db = CassandraConnectionProviderSingleton.getCassandraConnectionProvider(HOST,
        CassandraTestInstance.getPort(), KEYSPACE, USER_NAME,
        PASSWORD);
    taskInfoDAO = CassandraTaskInfoDAO.getInstance(db);
    taskErrorsDAO = CassandraTaskErrorsDAO.getInstance(db);
    subtaskDAO = NotificationsDAO.getInstance(db);
    processedRecordsDAO = ProcessedRecordsDAO.getInstance(db);
    reportService = new TaskExecutionReportServiceImpl(subtaskDAO, taskErrorsDAO, taskInfoDAO);
  }

  private void createBolt() {
    testedBolt = new NotificationBolt(HOST, CassandraTestInstance.getPort(), KEYSPACE, "", "");

    Map<String, Object> boltConfig = new HashMap<>();
    boltConfig.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("", ""));
    boltConfig.put(Config.STORM_ZOOKEEPER_PORT, "");
    boltConfig.put(Config.TOPOLOGY_NAME, "");
    testedBolt.prepare(boltConfig, null, collector);
  }

  @Test
  public void shouldProperlyExecuteRegularTuple() throws Exception {
    insertTaskToDB(TASK_ID, TOPOLOGY_NAME, 1, TaskState.QUEUED, "");
    Tuple tuple = createTestTuple(NotificationTuple.prepareNotification(createStormTaskTuple(TASK_ID, RESOURCE_1, false),
            RecordState.SUCCESS, "", ""));

    testedBolt.execute(tuple);

    TaskInfo taskProgress = reportService.getTaskProgress(TASK_ID);
    List<SubTaskInfo> notifications = reportService.getDetailedTaskReport(TASK_ID, 0, 100);
    assertThat(notifications, hasSize(1));
    assertEquals(TaskState.QUEUED, taskProgress.getState());
    assertEquals(1, taskProgress.getProcessedRecordsCount());
    assertEquals(0, taskProgress.getIgnoredRecordsCount());
    assertEquals(0, taskProgress.getDeletedRecordsCount());
    assertEquals(0, taskProgress.getProcessedErrorsCount());
    assertEquals(0, taskProgress.getDeletedErrorsCount());
  }

  @Test
  public void shouldProperlyExecuteIgnoredRecordTuple() throws Exception {
    insertTaskToDB(TASK_ID, TOPOLOGY_NAME, 1, TaskState.QUEUED, "");
    NotificationTuple notificationTuple = NotificationTuple.prepareNotification(createStormTaskTuple(TASK_ID, RESOURCE_1, false),
            RecordState.SUCCESS, "", "");
    notificationTuple.addParameter(PluginParameterKeys.IGNORED_RECORD, "true");
    Tuple tuple = createTestTuple(notificationTuple);

    testedBolt.execute(tuple);

    TaskInfo taskProgress = reportService.getTaskProgress(TASK_ID);
    List<SubTaskInfo> notifications = reportService.getDetailedTaskReport(TASK_ID, 0, 100);
    assertThat(notifications, hasSize(1));
    assertEquals(TaskState.QUEUED, taskProgress.getState());
    assertEquals(0, taskProgress.getProcessedRecordsCount());
    assertEquals(1, taskProgress.getIgnoredRecordsCount());
    assertEquals(0, taskProgress.getDeletedRecordsCount());
    assertEquals(0, taskProgress.getProcessedErrorsCount());
    assertEquals(0, taskProgress.getDeletedErrorsCount());
  }

  @Test
  public void shouldProperlyExecuteDeletedRecordTuple() throws Exception {
    insertTaskToDB(TASK_ID, TOPOLOGY_NAME, 1, TaskState.QUEUED, "");
    Tuple tuple = createTestTuple(NotificationTuple.prepareNotification(createStormTaskTuple(TASK_ID, RESOURCE_1, true),
            RecordState.SUCCESS, "", ""));

    testedBolt.execute(tuple);

    TaskInfo taskProgress = reportService.getTaskProgress(TASK_ID);
    List<SubTaskInfo> notifications = reportService.getDetailedTaskReport(TASK_ID, 0, 100);
    assertThat(notifications, hasSize(1));
    assertEquals(TaskState.QUEUED, taskProgress.getState());
    assertEquals(0, taskProgress.getProcessedRecordsCount());
    assertEquals(0, taskProgress.getIgnoredRecordsCount());
    assertEquals(1, taskProgress.getDeletedRecordsCount());
    assertEquals(0, taskProgress.getProcessedErrorsCount());
    assertEquals(0, taskProgress.getDeletedErrorsCount());
  }

  @Test
  public void shouldProperlyExecuteFailedTuple() throws Exception {
    insertTaskToDB(TASK_ID, TOPOLOGY_NAME, 1, TaskState.QUEUED, "");
    Tuple tuple = createTestTuple(NotificationTuple.prepareNotification(createStormTaskTuple(TASK_ID, RESOURCE_1, false),
            RecordState.ERROR, "", ""));

    testedBolt.execute(tuple);

    TaskInfo taskProgress = reportService.getTaskProgress(TASK_ID);
    List<SubTaskInfo> notifications = reportService.getDetailedTaskReport(TASK_ID, 0, 100);
    assertThat(notifications, hasSize(1));
    assertEquals(TaskState.QUEUED, taskProgress.getState());
    assertEquals(1, taskProgress.getProcessedRecordsCount());
    assertEquals(0, taskProgress.getIgnoredRecordsCount());
    assertEquals(0, taskProgress.getDeletedRecordsCount());
    assertEquals(1, taskProgress.getProcessedErrorsCount());
    assertEquals(0, taskProgress.getDeletedErrorsCount());
  }

  @Test
  public void shouldProperlyExecuteFailedDeletedRecordTuple() throws Exception {
    insertTaskToDB(TASK_ID, TOPOLOGY_NAME, 1, TaskState.QUEUED, "");
    Tuple tuple = createTestTuple(NotificationTuple.prepareNotification(createStormTaskTuple(TASK_ID, RESOURCE_1, true),
            RecordState.ERROR, "", ""));

    testedBolt.execute(tuple);

    TaskInfo taskProgress = reportService.getTaskProgress(TASK_ID);
    List<SubTaskInfo> notifications = reportService.getDetailedTaskReport(TASK_ID, 0, 100);
    assertThat(notifications, hasSize(1));
    assertEquals(TaskState.QUEUED, taskProgress.getState());
    assertEquals(0, taskProgress.getProcessedRecordsCount());
    assertEquals(0, taskProgress.getIgnoredRecordsCount());
    assertEquals(1, taskProgress.getDeletedRecordsCount());
    assertEquals(0, taskProgress.getProcessedErrorsCount());
    assertEquals(1, taskProgress.getDeletedErrorsCount());
  }

  @Test
  public void verifyOnlyOneNotificationForRepeatedRecord() throws Exception {
    long taskId = 1;
    insertTaskToDB(taskId, null, 10, TaskState.CURRENTLY_PROCESSING, "");

    Tuple tuple = createNotificationTuple(taskId, RecordState.SUCCESS);
    testedBolt.execute(tuple);
    testedBolt.execute(tuple);

    TaskInfo taskProgress = reportService.getTaskProgress(taskId);
    List<SubTaskInfo> notifications = reportService.getDetailedTaskReport(taskId, 0, 100);
    assertThat(notifications, hasSize(1));
    assertEquals(1, taskProgress.getProcessedRecordsCount());
  }


  @Test
  public void testSuccessfulNotificationFor101Tuples() throws Exception {
    //given

    long taskId = 1;
    int expectedSize = 101;
    String topologyName = null;
    TaskState taskState = TaskState.CURRENTLY_PROCESSING;
    String taskInfo = "";
    insertTaskToDB(taskId, topologyName, expectedSize, taskState, taskInfo);

    TaskInfo beforeExecute = reportService.getTaskProgress(taskId);
    testedBolt.execute(createNotificationTuple(taskId, RecordState.SUCCESS));

    for (int i = 0; i < 98; i++) {
      testedBolt.execute(createNotificationTuple(taskId, RecordState.SUCCESS));
    }
    testedBolt.execute(createNotificationTuple(taskId, RecordState.SUCCESS));

    TaskInfo afterOneHundredExecutions = reportService.getTaskProgress(taskId);
    testedBolt.execute(createNotificationTuple(taskId, RecordState.SUCCESS));
    assertEquals(0, beforeExecute.getProcessedRecordsCount());
    assertThat(beforeExecute.getState(), is(TaskState.CURRENTLY_PROCESSING));

    assertEquals(100, afterOneHundredExecutions.getProcessedRecordsCount());
    assertThat(afterOneHundredExecutions.getState(), is(TaskState.CURRENTLY_PROCESSING));
  }

  @Test
  public void testSuccessfulProgressUpdateAfterBoltRecreate() throws Exception {
    long taskId = 1;
    int expectedSize = 4;
    String topologyName = "";
    TaskState taskState = TaskState.QUEUED;
    String taskInfo = "";
    insertTaskToDB(taskId, topologyName, expectedSize, taskState, taskInfo);
    testedBolt.execute(createNotificationTuple(taskId, RecordState.SUCCESS));
    createBolt();
    testedBolt.execute(createNotificationTuple(taskId, RecordState.SUCCESS));
    testedBolt.execute(createNotificationTuple(taskId, RecordState.SUCCESS));
    await()
        .atMost(Durations.FIVE_SECONDS)
        .with()
        .pollInterval(Durations.ONE_HUNDRED_MILLISECONDS)
        .until(() -> {
          TaskInfo info = reportService.getTaskProgress(taskId);
          return info.getProcessedRecordsCount() == 3;
        });
    TaskInfo info = reportService.getTaskProgress(taskId);
    assertEquals(3, info.getProcessedRecordsCount());
    assertEquals(TaskState.QUEUED, info.getState());
    testedBolt.execute(createNotificationTuple(taskId, RecordState.SUCCESS));

    info = reportService.getTaskProgress(taskId);
    assertEquals(expectedSize, info.getProcessedRecordsCount());
    assertEquals(TaskState.QUEUED, info.getState());

  }

  @Test
  public void testValidNotificationAfterBoltRecreate() throws Exception {
    long taskId = 1;
    int expectedSize = 2;
    String topologyName = null;
    TaskState taskState = TaskState.CURRENTLY_PROCESSING;
    String taskInfo = "";
    insertTaskToDB(taskId, topologyName, 2, taskState, taskInfo);
    testedBolt.execute(createNotificationTuple(taskId, RecordState.SUCCESS));
    createBolt();
    testedBolt.execute(createNotificationTuple(taskId, RecordState.SUCCESS));

    assertEquals(expectedSize, subtaskDAO.getProcessedFilesCount(taskId));
  }

  @Test
  public void testValidErrorReportDataAfterBoltRecreate() throws Exception {
    long taskId = 1;
    String topologyName = null;
    TaskState taskState = TaskState.CURRENTLY_PROCESSING;
    String taskInfo = "";
    insertTaskToDB(taskId, topologyName, 2, taskState, taskInfo);
    testedBolt.execute(createNotificationTuple(taskId, RecordState.ERROR, RESOURCE_1));
    createBolt();
    testedBolt.execute(createNotificationTuple(taskId, RecordState.ERROR, RESOURCE_2));

    assertEquals(2, subtaskDAO.getProcessedFilesCount(taskId));
    TaskErrorsInfo errorReport = reportService.getGeneralTaskErrorReport(taskId, 100);
    assertEquals(1, errorReport.getErrors().size());
    assertEquals(2, errorReport.getErrors().get(0).getOccurrences());
    TaskErrorsInfo specificReport = reportService.getSpecificTaskErrorReport(taskId,
        errorReport.getErrors().get(0).getErrorType(), 100);
    assertEquals(1, specificReport.getErrors().size());
    TaskErrorInfo specificReportErrorInfo = specificReport.getErrors().get(0);
    assertEquals("text", specificReportErrorInfo.getMessage());
    assertEquals(2, specificReportErrorInfo.getErrorDetails().size());
    assertEquals(RESOURCE_1, specificReportErrorInfo.getErrorDetails().get(0).getIdentifier());
    assertEquals(RESOURCE_2, specificReportErrorInfo.getErrorDetails().get(1).getIdentifier());
  }

  @Test
  public void shouldProperlyRestoreAllCountersAfterBoltRecreate() throws Exception {
    insertTaskToDB(TASK_ID, TOPOLOGY_NAME, 6, TaskState.QUEUED, "");

    testedBolt.execute(createTestTuple(NotificationTuple.prepareNotification(createStormTaskTuple(TASK_ID, RESOURCE_1, false),
            RecordState.SUCCESS, "", "")));
    NotificationTuple notificationTuple = NotificationTuple.prepareNotification(createStormTaskTuple(TASK_ID, RESOURCE_2, false),
            RecordState.SUCCESS, "", "");
    notificationTuple.addParameter(PluginParameterKeys.IGNORED_RECORD, "true");
    testedBolt.execute(createTestTuple(notificationTuple));
    testedBolt.execute(createTestTuple(NotificationTuple.prepareNotification(createStormTaskTuple(TASK_ID, RESOURCE_3, true),
            RecordState.SUCCESS, "", "")));
    testedBolt.execute(createTestTuple(NotificationTuple.prepareNotification(createStormTaskTuple(TASK_ID, RESOURCE_4, false),
            RecordState.ERROR, "", "")));
    testedBolt.execute(createTestTuple(NotificationTuple.prepareNotification(createStormTaskTuple(TASK_ID, RESOURCE_5, true),
            RecordState.ERROR, "", "")));
    createBolt();
    testedBolt.execute(createTestTuple(NotificationTuple.prepareNotification(createStormTaskTuple(TASK_ID, RESOURCE_6, false),
            RecordState.SUCCESS, "", "")));

    TaskInfo taskProgress = reportService.getTaskProgress(TASK_ID);
    List<SubTaskInfo> notifications = reportService.getDetailedTaskReport(TASK_ID, 0, 100);
    assertThat(notifications, hasSize(6));
    assertEquals(TaskState.QUEUED, taskProgress.getState());
    assertEquals(3, taskProgress.getProcessedRecordsCount());
    assertEquals(1, taskProgress.getIgnoredRecordsCount());
    assertEquals(2, taskProgress.getDeletedRecordsCount());
    assertEquals(1, taskProgress.getProcessedErrorsCount());
    assertEquals(1, taskProgress.getDeletedErrorsCount());
  }

  private Tuple createNotificationTuple(long taskId, RecordState state) {
    String resource = "resource" + ++resourceCounter;
    return createNotificationTuple(taskId, state, resource);
  }

  private StormTaskTuple createStormTaskTuple(long taskId, String resource, boolean markAsDeleted) {
    return createStormTaskTuple(taskId, resource, markAsDeleted, "", "0");
  }

  private StormTaskTuple createStormTaskTuple(long taskId, String resource, boolean markAsDeleted, String processingTime) {
    return createStormTaskTuple(taskId, resource, markAsDeleted, "", processingTime);
  }

  private StormTaskTuple createStormTaskTuple(long taskId, String resource, boolean markAsDeleted, String resultResource, String processingTime) {
    StormTaskTuple tuple = new StormTaskTuple();
    tuple.setTaskId(taskId);
    tuple.setMarkedAsDeleted(markAsDeleted);
    tuple.setFileUrl(resource);
    tuple.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, processingTime);
    tuple.addParameter(PluginParameterKeys.OUTPUT_URL, resultResource);
    return tuple;
  }

  private Tuple createNotificationTuple(long taskId, RecordState state, String resource) {
    String text = "text";
    String additionalInformation = "additionalInformation";
    String resultResource = "";
    return createTestTuple(NotificationTuple.prepareNotificationWithResultResource(createStormTaskTuple(taskId, resource, false, resultResource, "1"), state, text,
            additionalInformation));
  }


  @Test
  public void testNotificationProgressPercentage() throws Exception {
    TaskExecutionReportServiceImpl reportService = new TaskExecutionReportServiceImpl(subtaskDAO, taskErrorsDAO, taskInfoDAO);
    long taskId = 1;
    int expectedSize = 330;
    int errors = 5;
    int middle = (int) (Math.random() * expectedSize);
    String topologyName = "";
    TaskState taskState = TaskState.QUEUED;
    String taskInfo = "";
    insertTaskToDB(taskId, topologyName, expectedSize, taskState, taskInfo);

    //when
    List<Tuple> tuples = prepareTuples(taskId, expectedSize, errors);

    TaskInfo beforeExecute = reportService.getTaskProgress(taskId);
    TaskInfo middleExecute = null;

    for (var i = 0; i < tuples.size(); i++) {
      if (i == middle - 1) {
        testedBolt.execute(tuples.get(i));
        await()
            .atMost(Durations.FIVE_SECONDS)
            .with()
            .pollInterval(Durations.ONE_HUNDRED_MILLISECONDS)
            .until(() -> {
              TaskInfo mE = reportService.getTaskProgress(taskId);
              return mE.getState() == TaskState.QUEUED;
            });
        middleExecute = reportService.getTaskProgress(taskId);
      } else {
        testedBolt.execute(tuples.get(i));
      }
    }

    TaskInfo afterExecute = reportService.getTaskProgress(taskId);

    //then
    assertEquals(0, beforeExecute.getProcessedRecordsCount());
    assertThat(beforeExecute.getState(), is(TaskState.QUEUED));
    assertEquals(0, beforeExecute.getProcessedRecordsCount());

    if (middleExecute != null) {
      assertEquals(middleExecute.getProcessedRecordsCount(), (middle));
      assertThat(middleExecute.getState(), is(TaskState.QUEUED));
    }
    int totalProcessed = expectedSize;
    assertEquals(afterExecute.getProcessedRecordsCount(), totalProcessed + (expectedSize - totalProcessed));
    assertThat(afterExecute.getState(), is(TaskState.QUEUED));
  }

  @Test
  public void testNotificationForErrors() throws Exception {
    TaskExecutionReportServiceImpl reportService = new TaskExecutionReportServiceImpl(subtaskDAO, taskErrorsDAO, taskInfoDAO);
    long taskId = 1;
    int expectedSize = 20;
    int errors = 9;
    String topologyName = null;
    TaskState taskState = TaskState.CURRENTLY_PROCESSING;
    String taskInfo = "";
    insertTaskToDB(taskId, topologyName, expectedSize, taskState, taskInfo);

    //when
    List<Tuple> tuples = prepareTuples(taskId, expectedSize, errors);

    TaskInfo beforeExecute = reportService.getTaskProgress(taskId);

    for (Tuple tuple : tuples) {
      testedBolt.execute(tuple);
    }

    TaskErrorsInfo errorsInfo = reportService.getGeneralTaskErrorReport(taskId, 0);

    //then
    assertEquals(0, beforeExecute.getProcessedRecordsCount());
    assertThat(beforeExecute.getState(), is(TaskState.CURRENTLY_PROCESSING));
    assertEquals(0, beforeExecute.getProcessedErrorsCount());

    assertEquals(1, errorsInfo.getErrors().size());
    assertEquals(errorsInfo.getErrors().get(0).getOccurrences(), errors);
  }

  @Test
  public void shouldProperlyExecuteTupleWithReports() throws AccessDeniedOrObjectDoesNotExistException {
    TaskExecutionReportServiceImpl reportService = new TaskExecutionReportServiceImpl(subtaskDAO, taskErrorsDAO, taskInfoDAO);
    long taskId = 1;
    TaskState taskState = TaskState.CURRENTLY_PROCESSING;
    String taskInfo = "";
    insertTaskToDB(taskId, null, 1, taskState, taskInfo);

    //when
    Tuple tuple = prepareTupleWithReport(taskId, 10, 10);

    TaskInfo beforeExecute = reportService.getTaskProgress(taskId);

    testedBolt.execute(tuple);

    TaskErrorsInfo errorsInfo = reportService.getGeneralTaskErrorReport(taskId, 0);

    assertEquals(0, beforeExecute.getProcessedRecordsCount());
    assertThat(beforeExecute.getState(), is(TaskState.CURRENTLY_PROCESSING));
    assertEquals(0, beforeExecute.getProcessedErrorsCount());

    List<TaskErrorInfo> errors = errorsInfo.getErrors();
    assertEquals(3, errors.size());
    int sumOfOccurrences = errors.stream().map(TaskErrorInfo::getOccurrences).reduce(0, Integer::sum);
    assertEquals(21, sumOfOccurrences);
  }

  @Test
  public void shouldProperlyExecuteTupleWithExpectedSizeNotAvailableAtTheBeginning()
      throws AccessDeniedOrObjectDoesNotExistException {
    insertTaskToDB(TASK_ID, TOPOLOGY_NAME, -1, TaskState.PROCESSING_BY_REST_APPLICATION, "");
    testedBolt.execute(createNotificationTuple(1L, RecordState.SUCCESS));
    taskInfoDAO.updateStatusExpectedSize(TASK_ID, TaskState.QUEUED, 2);
    testedBolt.execute(createNotificationTuple(1L, RecordState.SUCCESS));

    TaskInfo taskProgress = reportService.getTaskProgress(TASK_ID);
    List<SubTaskInfo> notifications = reportService.getDetailedTaskReport(TASK_ID, 0, 100);
    assertThat(notifications, hasSize(2));
    assertEquals(TaskState.QUEUED, taskProgress.getState());
    assertEquals(2, taskProgress.getProcessedRecordsCount());
    assertEquals(0, taskProgress.getIgnoredRecordsCount());
    assertEquals(0, taskProgress.getDeletedRecordsCount());
    assertEquals(0, taskProgress.getProcessedErrorsCount());
    assertEquals(0, taskProgress.getDeletedErrorsCount());
    assertEquals(2, taskProgress.getExpectedRecordsNumber());
  }

  @Test
  public void shouldProperlyExecuteTupleWithHugeErrorsList() throws AccessDeniedOrObjectDoesNotExistException {

    insertTaskToDB(TASK_ID, TOPOLOGY_NAME, 1, TaskState.QUEUED, "");
    NotificationTuple notificationTuple = NotificationTuple.prepareNotificationWithResultResource(createStormTaskTuple(TASK_ID, RESOURCE_1, false, "", "0"), RecordState.SUCCESS,
            "",
            "");
    notificationTuple.addParameter(NotificationParameterKeys.UNIFIED_ERROR_MESSAGE, "Unified_error_message");
    notificationTuple.addParameter(NotificationParameterKeys.EXCEPTION_ERROR_MESSAGE, LARGE_STACK_TRACE);

    Tuple tuple = createTestTuple(notificationTuple);

    testedBolt.execute(tuple);

    TaskInfo taskProgress = reportService.getTaskProgress(TASK_ID);
    List<SubTaskInfo> notifications = reportService.getDetailedTaskReport(TASK_ID, 0, 100);
    assertThat(notifications, hasSize(1));
    assertEquals(TaskState.QUEUED, taskProgress.getState());
    assertEquals(1, taskProgress.getProcessedRecordsCount());
    assertEquals(0, taskProgress.getIgnoredRecordsCount());
    assertEquals(0, taskProgress.getDeletedRecordsCount());
    assertEquals(0, taskProgress.getProcessedErrorsCount());
    assertEquals(0, taskProgress.getDeletedErrorsCount());
    TaskErrorsInfo errorsInfo = reportService.getGeneralTaskErrorReport(1, 1000);
    errorsInfo.getErrors().forEach(
        taskErrorInfo -> taskErrorInfo.getErrorDetails().forEach(
            errorDetails -> assertTrue(
                errorDetails.getAdditionalInfo().length() <= NotificationTupleHandler.MAX_STACKTRACE_LENGTH)));
  }

  private void insertTaskToDB(long taskId, String topologyName, int expectedSize, TaskState state,
      String info)
      throws NoHostAvailableException, QueryExecutionException {
    taskInfoDAO.insert(
        TaskInfo.builder()
                .id(taskId)
                .topologyName(topologyName)
                .expectedRecordsNumber(expectedSize)
                .state(state)
                .stateDescription(info)
                .build());

  }

  private List<Tuple> prepareTuples(long taskId, int size, int errors) {
    String resource = "resource";
    String text = "text";
    String additionalInformation = "additionalInformation";
    String resultResource = "";
    List<Tuple> result = new ArrayList<>();

    if (errors > size) {
      errors = size;
    }

    for (int i = 0; i < errors; i++) {
      result.add(createTestTuple(NotificationTuple.prepareNotificationWithResultResource(createStormTaskTuple(TASK_ID, resource + i, false, resultResource, "1"),
              RecordState.ERROR, text, additionalInformation)));
    }

    while (result.size() < size) {
      result.add(createTestTuple(NotificationTuple.prepareNotificationWithResultResource(createStormTaskTuple(TASK_ID, String.valueOf(result.size()), false, resultResource, "1"),
              RecordState.SUCCESS, text, additionalInformation)));
    }
    return result;
  }

  private Tuple prepareTupleWithReport(long taskId, int errorReportCount, int warningReportCount) {
    NotificationTuple notificationTuple = NotificationTuple.prepareNotificationWithResultResource(createStormTaskTuple(taskId, "resource", false, "", "1"),
            RecordState.ERROR, "text", "additionalInformation");
    HashSet<Report> reports = new HashSet<>();
    for (int i = 0; i < errorReportCount; i++) {
      reports.add(Report.buildEnrichmentError().withMessage("EnrichmentError").withValue(String.valueOf(i)).build());
    }
    for (int i = 0; i < warningReportCount; i++) {
      reports.add(
              new Random().nextInt() == 1 ? Report.buildEnrichmentWarn().withMessage("EnrichmentWarning").withValue(String.valueOf(i))
                      .build()
                      : Report.buildDereferenceWarn().withMessage("DereferenceWarning").withValue(String.valueOf(i)).build());
    }
    notificationTuple.addReports(reports);
    return createTestTuple(notificationTuple);
  }

  private TaskInfo createTaskInfo(long taskId, int containElement, String topologyName, TaskState state, String info,
      Date sentTime, Date startTime, Date finishTime) {
    TaskInfo expectedTaskInfo = TaskInfo.builder()
                                        .id(taskId)
                                        .topologyName(topologyName)
                                        .state(state)
                                        .stateDescription(info)
                                        .sentTimestamp(sentTime)
                                        .startTimestamp(startTime)
                                        .finishTimestamp(finishTime)
                                        .build();
    expectedTaskInfo.setExpectedRecordsNumber(containElement);
    return expectedTaskInfo;
  }


  private Tuple createTestTuple(NotificationTuple tuple) {

    String recordId = (String) tuple.getParameters().get(NotificationParameterKeys.RESOURCE);
    if (recordId != null) {
      processedRecordsDAO.insert(tuple.getTaskId(), recordId, 1, "", "",
          RecordState.QUEUED.toString(), "", "");
    }
    Values testValue = tuple.toStormTuple();
    TopologyBuilder builder = new TopologyBuilder();
    @SuppressWarnings("unchecked")
    GeneralTopologyContext topologyContext = new GeneralTopologyContext(builder.createTopology(), new Config(), new HashMap(),
        new HashMap(), new HashMap(), "") {
      @Override
      public Fields getComponentOutputFields(String componentId, String streamId) {
        return NotificationTuple.getFields();
      }
    };
    return new TupleImpl(topologyContext, testValue, "BoltTest", 1, "");
  }

  private static final String LARGE_STACK_TRACE =
      "Exception while processing the resource: https://repository.dri.ie/images/1831s834g:xd07wh11t/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:xd07wh11t/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:zc786d51j/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:zc786d51j/full/full/0/default.jpg because of: java.net.SocketTimeoutException:\n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:mc883d67p/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:mc883d67p/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:1r66xq549/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:1r66xq549/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:td96zr90z/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:td96zr90z/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:vq28cb83b/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:vq28cb83b/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:66839t35g/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:66839t35g/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:bg25n4476/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:bg25n4476/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:57130w96c/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:57130w96c/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:t148v655j/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:t148v655j/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:3x81mb046/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:3x81mb046/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:qn603t39b/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:qn603t39b/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:gb19tw222/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:gb19tw222/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:qf862170h/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:qf862170h/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:hq388b99g/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:hq388b99g/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:n2979n54g/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:n2979n54g/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:sn00qn183/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:sn00qn183/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Exception while processing the resource: https://repository.dri.ie/images/1831s834g:xd07wh11t/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:xd07wh11t/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:zc786d51j/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:zc786d51j/full/full/0/default.jpg because of: java.net.SocketTimeoutException:\n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:mc883d67p/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:mc883d67p/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:1r66xq549/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:1r66xq549/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:2b894260c/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:2b894260c/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:c534vc358/full/full/0/default.jpg.\n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:c534vc358/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:3b59dz992/full/full/0/default.jpg.\n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:3b59dz992/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:1z410h23x/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:1z410h23x/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:fx71q9867/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:fx71q9867/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:1n79wt71q/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:1n79wt71q/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:k643qq586/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:k643qq586/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:gq67zf59q/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:gq67zf59q/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:vd678p31s/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:vd678p31s/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:v9807s475/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:v9807s475/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:25152890c/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:25152890c/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:k930rm42j/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:k930rm42j/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:zp399303w/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:zp399303w/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:zs269z87s/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:zs269z87s/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:7p88s5953/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:7p88s5953/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:td96zr90z/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:td96zr90z/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:vq28cb83b/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:vq28cb83b/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:66839t35g/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:66839t35g/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:bg25n4476/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:bg25n4476/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:57130w96c/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:57130w96c/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:t148v655j/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:t148v655j/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:3x81mb046/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:3x81mb046/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:qn603t39b/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:qn603t39b/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:gb19tw222/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:gb19tw222/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:qf862170h/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:qf862170h/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:hq388b99g/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:hq388b99g/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:n2979n54g/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:n2979n54g/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:sn00qn183/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:sn00qn183/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Exception while processing the resource: https://repository.dri.ie/images/1831s834g:xd07wh11t/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:xd07wh11t/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:zc786d51j/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:zc786d51j/full/full/0/default.jpg because of: java.net.SocketTimeoutException:\n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:mc883d67p/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:mc883d67p/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:1r66xq549/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:1r66xq549/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:2b894260c/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:2b894260c/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:c534vc358/full/full/0/default.jpg.\n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:c534vc358/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:3b59dz992/full/full/0/default.jpg.\n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:3b59dz992/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:1z410h23x/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:1z410h23x/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:fx71q9867/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:fx71q9867/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:1n79wt71q/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:1n79wt71q/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:k643qq586/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:k643qq586/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:gq67zf59q/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:gq67zf59q/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:vd678p31s/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:vd678p31s/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:v9807s475/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:v9807s475/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:25152890c/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:25152890c/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:k930rm42j/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:k930rm42j/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:zp399303w/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:zp399303w/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:zs269z87s/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:zs269z87s/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:7p88s5953/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:7p88s5953/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:td96zr90z/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:td96zr90z/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:vq28cb83b/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:vq28cb83b/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:66839t35g/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:66839t35g/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:bg25n4476/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:bg25n4476/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:57130w96c/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:57130w96c/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:t148v655j/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:t148v655j/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:3x81mb046/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:3x81mb046/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:qn603t39b/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:qn603t39b/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:gb19tw222/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:gb19tw222/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:qf862170h/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:qf862170h/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:hq388b99g/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:hq388b99g/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:n2979n54g/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:n2979n54g/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:sn00qn183/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:sn00qn183/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Exception while processing the resource: https://repository.dri.ie/images/1831s834g:xd07wh11t/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:xd07wh11t/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:zc786d51j/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:zc786d51j/full/full/0/default.jpg because of: java.net.SocketTimeoutException:\n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:mc883d67p/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:mc883d67p/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:1r66xq549/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:1r66xq549/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:2b894260c/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:2b894260c/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:c534vc358/full/full/0/default.jpg.\n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:c534vc358/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:3b59dz992/full/full/0/default.jpg.\n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:3b59dz992/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:1z410h23x/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:1z410h23x/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:fx71q9867/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:fx71q9867/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:1n79wt71q/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:1n79wt71q/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:k643qq586/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:k643qq586/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:gq67zf59q/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:gq67zf59q/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:vd678p31s/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:vd678p31s/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:v9807s475/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:v9807s475/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:25152890c/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:25152890c/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:k930rm42j/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:k930rm42j/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:zp399303w/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:zp399303w/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:zs269z87s/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:zs269z87s/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:7p88s5953/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:7p88s5953/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:td96zr90z/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:td96zr90z/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:vq28cb83b/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:vq28cb83b/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:66839t35g/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:66839t35g/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:bg25n4476/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:bg25n4476/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:57130w96c/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:57130w96c/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:t148v655j/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:t148v655j/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:3x81mb046/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:3x81mb046/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:qn603t39b/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:qn603t39b/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:gb19tw222/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:gb19tw222/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:qf862170h/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:qf862170h/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:hq388b99g/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:hq388b99g/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:n2979n54g/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:n2979n54g/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:sn00qn183/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:sn00qn183/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Exception while processing the resource: https://repository.dri.ie/images/1831s834g:xd07wh11t/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:xd07wh11t/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:zc786d51j/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:zc786d51j/full/full/0/default.jpg because of: java.net.SocketTimeoutException:\n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:mc883d67p/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:mc883d67p/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:1r66xq549/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:1r66xq549/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:2b894260c/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:2b894260c/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:c534vc358/full/full/0/default.jpg.\n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:c534vc358/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:3b59dz992/full/full/0/default.jpg.\n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:3b59dz992/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:1z410h23x/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:1z410h23x/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:fx71q9867/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:fx71q9867/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:1n79wt71q/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:1n79wt71q/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:k643qq586/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:k643qq586/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:gq67zf59q/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:gq67zf59q/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:vd678p31s/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:vd678p31s/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:v9807s475/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:v9807s475/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:25152890c/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:25152890c/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:k930rm42j/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:k930rm42j/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:zp399303w/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:zp399303w/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:zs269z87s/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:zs269z87s/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:7p88s5953/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:7p88s5953/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:td96zr90z/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:td96zr90z/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:vq28cb83b/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:vq28cb83b/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:66839t35g/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:66839t35g/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:bg25n4476/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:bg25n4476/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:57130w96c/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:57130w96c/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:t148v655j/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:t148v655j/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:3x81mb046/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:3x81mb046/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:qn603t39b/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:qn603t39b/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:gb19tw222/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:gb19tw222/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:qf862170h/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:qf862170h/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:hq388b99g/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:hq388b99g/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:n2979n54g/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:n2979n54g/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:sn00qn183/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:sn00qn183/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Exception while processing the resource: https://repository.dri.ie/images/1831s834g:xd07wh11t/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:xd07wh11t/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:zc786d51j/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:zc786d51j/full/full/0/default.jpg because of: java.net.SocketTimeoutException:\n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:mc883d67p/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:mc883d67p/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:1r66xq549/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:1r66xq549/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:2b894260c/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:2b894260c/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:c534vc358/full/full/0/default.jpg.\n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:c534vc358/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:3b59dz992/full/full/0/default.jpg.\n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:3b59dz992/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:1z410h23x/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:1z410h23x/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:fx71q9867/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:fx71q9867/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:1n79wt71q/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:1n79wt71q/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:k643qq586/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:k643qq586/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:gq67zf59q/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:gq67zf59q/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:vd678p31s/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:vd678p31s/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:v9807s475/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:v9807s475/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:25152890c/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:25152890c/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:k930rm42j/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:k930rm42j/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:zp399303w/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:zp399303w/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:zs269z87s/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:zs269z87s/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:7p88s5953/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:7p88s5953/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:td96zr90z/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:td96zr90z/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:vq28cb83b/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:vq28cb83b/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:66839t35g/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:66839t35g/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:bg25n4476/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:bg25n4476/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:57130w96c/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:57130w96c/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:t148v655j/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:t148v655j/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:3x81mb046/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:3x81mb046/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:qn603t39b/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:qn603t39b/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:gb19tw222/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:gb19tw222/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:qf862170h/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:qf862170h/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:hq388b99g/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:hq388b99g/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:n2979n54g/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:n2979n54g/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:sn00qn183/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:sn00qn183/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Exception while processing the resource: https://repository.dri.ie/images/1831s834g:xd07wh11t/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:xd07wh11t/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:zc786d51j/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:zc786d51j/full/full/0/default.jpg because of: java.net.SocketTimeoutException:\n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:mc883d67p/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:mc883d67p/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:1r66xq549/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:1r66xq549/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:2b894260c/full/full/0/default.jpg. \n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:2b894260c/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:c534vc358/full/full/0/default.jpg.\n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:c534vc358/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:3b59dz992/full/full/0/default.jpg.\n"
          +
          "The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:3b59dz992/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:1z410h23x/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:1z410h23x/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:fx71q9867/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:fx71q9867/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:1n79wt71q/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:1n79wt71q/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:k643qq586/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:k643qq586/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:gq67zf59q/full/full/0/default.jpg.\n"
          +
          " The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:gq67zf59q/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out, Exception while processing the resource: https://repository.dri.ie/images/1831s834g:sn00qn183/full/full/0/default.jpg. The full error is: Problem while processing https://repository.dri.ie/images/1831s834g:sn00qn183/full/full/0/default.jpg because of: java.net.SocketTimeoutException: \n"
          +
          "Read timed out";
}