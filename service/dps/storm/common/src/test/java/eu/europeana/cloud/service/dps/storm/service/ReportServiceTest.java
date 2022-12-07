package eu.europeana.cloud.service.dps.storm.service;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.common.model.dps.TaskErrorInfo;
import eu.europeana.cloud.common.model.dps.TaskErrorsInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.NotificationsDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
import eu.europeana.cloud.test.CassandraTestInstance;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;


public class ReportServiceTest extends CassandraTestBase {

  private static final long TASK_ID_LONG = 111;
  private static final String TASK_ID = String.valueOf(TASK_ID_LONG);
  private static final String TOPOLOGY_NAME = "some_topology";
  private static final String ERROR_TYPE = "1c71e7b0-7633-11ed-b1fe-a7fdf50126b2";
  private static final String ERROR_TYPE1 = "1c71e7b0-7633-11ed-b1fe-a7fdf50126b3";
  private static final String ERROR_TYPE2 = "1c71e7b0-7633-11ed-b1fe-a7fdf50126b4";
  private ReportService service;
  private NotificationsDAO subtaskInfoDao;
  private CassandraTaskInfoDAO taskInfoDAO;
  private CassandraTaskErrorsDAO errorsDAO;

  @Before
  public void setup() {
    CassandraConnectionProvider db = new CassandraConnectionProvider(HOST, CassandraTestInstance.getPort(), KEYSPACE, USER,
        PASSWORD);
    service = new ReportService(new CassandraConnectionProvider(HOST, CassandraTestInstance.getPort(), KEYSPACE, USER, PASSWORD));
    subtaskInfoDao = NotificationsDAO.getInstance(db);
    taskInfoDAO = CassandraTaskInfoDAO.getInstance(db);
    errorsDAO = CassandraTaskErrorsDAO.getInstance(db);
  }

  @Test
  public void shouldReturnTaskProgressWhenTaskExists() throws AccessDeniedOrObjectDoesNotExistException {
    createAndStoreTaskInfo();
    TaskInfo taskInfo = service.getTaskProgress(TASK_ID);
    assertEquals(TaskState.QUEUED, taskInfo.getState());
    assertEquals(TASK_ID_LONG, taskInfo.getId());
  }

  @Test
  public void shouldThrowExceptionWhenTaskDoesntExistOrIsFromDifferentTopology() {
    assertThrows(AccessDeniedOrObjectDoesNotExistException.class, () -> service.getTaskProgress(TASK_ID));
    assertThrows(AccessDeniedOrObjectDoesNotExistException.class, () -> service.checkIfTaskExists(TASK_ID, TOPOLOGY_NAME));
    createAndStoreTaskInfo();
    assertThrows(AccessDeniedOrObjectDoesNotExistException.class, () -> service.checkIfTaskExists(TASK_ID, "any_topology"));
  }

  @Test
  public void shouldCheckIfReportExists() {
    assertFalse(service.checkIfReportExists(TASK_ID));
    createAndStoreErrorType();
    assertTrue(service.checkIfReportExists(TASK_ID));
  }

  @Test
  public void shouldReturnSpecificTaskErrorReturn() throws AccessDeniedOrObjectDoesNotExistException {
    createAndStoreErrorType();
    createAndStoreErrorNotification();
    TaskErrorsInfo errorsInfo = service.getSpecificTaskErrorReport(TASK_ID, ERROR_TYPE, 5);
    assertEquals(1, errorsInfo.getErrors().size());
  }

  @Test
  public void shouldThrowExceptionWhenSpecificTaskErrorDoesntExistOrErrorNotificationDoesntExist() {
    assertThrows(AccessDeniedOrObjectDoesNotExistException.class,
        () -> service.getSpecificTaskErrorReport(TASK_ID, ERROR_TYPE, 5));
    createAndStoreErrorType();
    assertThrows(AccessDeniedOrObjectDoesNotExistException.class,
        () -> service.getSpecificTaskErrorReport(TASK_ID, ERROR_TYPE, 5));
  }

  @Test
  public void shouldReturnGeneralTaskErrorReport() throws AccessDeniedOrObjectDoesNotExistException {
    assertEquals(0, service.getGeneralTaskErrorReport(TASK_ID, 5).getErrors().size());
    createAndStoreErrorType();
    createAndStoreErrorNotification();
    createAndStoreErrorType(ERROR_TYPE1);
    createAndStoreErrorNotification(ERROR_TYPE1);
    createAndStoreErrorType(ERROR_TYPE2);
    createAndStoreErrorNotification(ERROR_TYPE2);
    List<TaskErrorInfo> errorInfos = service.getGeneralTaskErrorReport(TASK_ID, 5).getErrors();
    assertEquals(3, errorInfos.size());
    assertTrue(errorInfos.stream().anyMatch(errorInfo -> ERROR_TYPE.equals(errorInfo.getErrorType())));
    assertTrue(errorInfos.stream().anyMatch(errorInfo -> ERROR_TYPE1.equals(errorInfo.getErrorType())));
    assertTrue(errorInfos.stream().anyMatch(errorInfo -> ERROR_TYPE2.equals(errorInfo.getErrorType())));
  }


  @Test
  public void shouldReturnEmptyReportWhenThereIsNoDataWhenQueryingOneRecord() {
    List<SubTaskInfo> report = service.getDetailedTaskReport(TASK_ID, 1, 1);

    assertThat(report, hasSize(0));
  }

  @Test
  public void shouldReturnEmptyReportWhenThereIsNoDataWhenQueryingOneBucket() {
    List<SubTaskInfo> report = service.getDetailedTaskReport(TASK_ID, 1, 100);

    assertThat(report, hasSize(0));
  }

  @Test
  public void shouldReturnEmptyReportWhenThereIsNoDataWhenQueryingManyBuckets() {
    List<SubTaskInfo> report = service.getDetailedTaskReport(TASK_ID, 1, NotificationsDAO.BUCKET_SIZE * 3);

    assertThat(report, hasSize(0));
  }

  @Test
  public void shouldReturnValidReportWhenQueryingOneRecord() {
    SubTaskInfo info = createAndStoreSubtaskInfo(1);

    List<SubTaskInfo> report = service.getDetailedTaskReport(TASK_ID, 1, 1);

    assertThat(report, hasSize(1));
    assertEquals(info, report.get(0));
  }

  @Test
  public void shouldReturnValidReportWhenQueryingOneBucket() {
    shouldReturnValidReportWhenQueryingBucketTemplate(100, 100);
  }

  @Test
  public void shouldReturnValidReportWhenQueryingOneBucketAtBorderOfData() {
    shouldReturnValidReportWhenQueryingBucketTemplate(100, 200);
  }

  @Test
  public void shouldReturnValidReportWhenQueryingManyBuckets() {
    shouldReturnValidReportWhenQueryingBucketTemplate(30000, 30000);
  }

  @Test
  public void shouldReturnValidReportWhenQueryingDataOverlappingBuckets() {
    shouldReturnValidReportWhenQueryingBucketTemplate(25000, 50000);
  }

  @Test
  public void shouldReturnValidReportWhenQueryingPartOfDataInOneBucketAtBorderOfData() {
    List<SubTaskInfo> infoList =
        createAndStoreSubtaskInfoRange(300).subList(100 - 1, 200);

    List<SubTaskInfo> report = service.getDetailedTaskReport(TASK_ID, 100, 200);

    assertEquals(Lists.reverse(infoList), report);
  }


  @Test
  public void shouldReturnValidReportWhenQueryingPartOfDataInFurtherBucket() {
    List<SubTaskInfo> infoList =
        createAndStoreSubtaskInfoRange(40000).subList(21000 - 1, 22000);

    List<SubTaskInfo> report = service.getDetailedTaskReport(TASK_ID, 21000, 22000);

    assertEquals(Lists.reverse(infoList), report);
  }

  private void shouldReturnValidReportWhenQueryingBucketTemplate(int preparedReportSize, int requestTo) {
    List<SubTaskInfo> infoList = createAndStoreSubtaskInfoRange(preparedReportSize);

    List<SubTaskInfo> report = service.getDetailedTaskReport(TASK_ID, 1, requestTo);

    assertEquals(Lists.reverse(infoList), report);
  }

  private List<SubTaskInfo> createAndStoreSubtaskInfoRange(int to) {
    List<SubTaskInfo> result = new ArrayList<>();
    for (int i = 1; i <= to; i++) {
      result.add(createAndStoreSubtaskInfo(i));
    }
    return result;
  }

  private void createAndStoreErrorType(String errorType) {
    errorsDAO.insertErrorCounter(TASK_ID_LONG, errorType, 5);
  }

  private void createAndStoreErrorType() {
    createAndStoreErrorType(ERROR_TYPE);
  }

  private void createAndStoreErrorNotification() {
    createAndStoreErrorNotification(ERROR_TYPE);
  }

  private void createAndStoreErrorNotification(String errorType) {
    errorsDAO.insertError(TASK_ID_LONG, errorType, "some_error_message", "some_resource", "some_additional_information");
  }

  private void createAndStoreTaskInfo() {
    TaskInfo exampleTaskInfo = new TaskInfo();
    exampleTaskInfo.setId(TASK_ID_LONG);
    exampleTaskInfo.setState(TaskState.QUEUED);
    exampleTaskInfo.setTopologyName(TOPOLOGY_NAME);
    taskInfoDAO.insert(exampleTaskInfo);
  }

  private SubTaskInfo createAndStoreSubtaskInfo(int resourceNum) {
    SubTaskInfo info = new SubTaskInfo(resourceNum, "resource" + resourceNum, RecordState.QUEUED, "info", "additionalInformation",
        "europeanaId", 0L, "resultResource" + resourceNum);
    subtaskInfoDao.insert(info.getResourceNum(), TASK_ID_LONG, TOPOLOGY_NAME,
        info.getResource(), info.getRecordState().toString(), info.getInfo(),
        Map.of(
            NotificationsDAO.STATE_DESCRIPTION_KEY, info.getAdditionalInformations(),
            NotificationsDAO.EUROPEANA_ID_KEY, info.getEuropeanaId(),
            NotificationsDAO.PROCESSING_TIME_KEY, String.valueOf(info.getProcessingTime())
        ),
        info.getResultResource());
    return info;
  }

}