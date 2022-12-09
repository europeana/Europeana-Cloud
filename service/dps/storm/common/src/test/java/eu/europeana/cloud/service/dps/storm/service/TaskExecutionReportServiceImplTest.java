package eu.europeana.cloud.service.dps.storm.service;

import static eu.europeana.cloud.service.dps.storm.utils.ServiceAndDAOTestUtils.ERROR_TYPE;
import static eu.europeana.cloud.service.dps.storm.utils.ServiceAndDAOTestUtils.TASK_ID;
import static eu.europeana.cloud.service.dps.storm.utils.ServiceAndDAOTestUtils.TOPOLOGY_NAME;
import static eu.europeana.cloud.service.dps.storm.utils.ServiceAndDAOTestUtils.createAndStoreErrorNotification;
import static eu.europeana.cloud.service.dps.storm.utils.ServiceAndDAOTestUtils.createAndStoreErrorType;
import static eu.europeana.cloud.service.dps.storm.utils.ServiceAndDAOTestUtils.createAndStoreNotification;
import static eu.europeana.cloud.service.dps.storm.utils.ServiceAndDAOTestUtils.createAndStoreSubtaskInfoRange;
import static eu.europeana.cloud.service.dps.storm.utils.ServiceAndDAOTestUtils.createAndStoreTaskInfo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
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
import eu.europeana.cloud.service.dps.storm.utils.ServiceAndDAOTestUtils;
import eu.europeana.cloud.test.CassandraTestInstance;
import java.util.List;
import org.junit.Before;
import org.junit.Test;


public class TaskExecutionReportServiceImplTest extends CassandraTestBase {

  private static final String ERROR_TYPE1 = "1c71e7b0-7633-11ed-b1fe-a7fdf50126b3";
  private static final String ERROR_TYPE2 = "1c71e7b0-7633-11ed-b1fe-a7fdf50126b4";
  private TaskExecutionReportServiceImpl service;
  private NotificationsDAO notificationsDAO;
  private CassandraTaskInfoDAO taskInfoDAO;
  private CassandraTaskErrorsDAO errorsDAO;

  @Before
  public void setup() {
    CassandraConnectionProvider db = CassandraConnectionProviderSingleton.getCassandraConnectionProvider(HOST,
        CassandraTestInstance.getPort(), KEYSPACE, USER,
        PASSWORD);
    notificationsDAO = NotificationsDAO.getInstance(db);
    taskInfoDAO = CassandraTaskInfoDAO.getInstance(db);
    errorsDAO = CassandraTaskErrorsDAO.getInstance(db);
    service = new TaskExecutionReportServiceImpl(notificationsDAO, errorsDAO, taskInfoDAO);
  }

  @Test
  public void shouldReturnTaskProgressWhenTaskExists() throws AccessDeniedOrObjectDoesNotExistException {
    createAndStoreTaskInfo(taskInfoDAO);
    TaskInfo taskInfo = service.getTaskProgress(TASK_ID);
    assertEquals(TaskState.QUEUED, taskInfo.getState());
    assertEquals(ServiceAndDAOTestUtils.TASK_ID, taskInfo.getId());
  }

  @Test
  public void shouldThrowExceptionWhenTaskDoesntExistOrIsFromDifferentTopology() {
    assertThrows(AccessDeniedOrObjectDoesNotExistException.class, () -> service.getTaskProgress(TASK_ID));
    assertThrows(AccessDeniedOrObjectDoesNotExistException.class, () -> service.checkIfTaskExists(TASK_ID, TOPOLOGY_NAME));
    createAndStoreTaskInfo(taskInfoDAO);
    assertThrows(AccessDeniedOrObjectDoesNotExistException.class, () -> service.checkIfTaskExists(TASK_ID, "any_topology"));
  }

  @Test
  public void shouldCheckIfReportExists() {
    assertFalse(service.checkIfReportExists(TASK_ID));
    createAndStoreErrorType(errorsDAO);
    assertTrue(service.checkIfReportExists(TASK_ID));
  }

  @Test
  public void shouldReturnSpecificTaskErrorReturn() throws AccessDeniedOrObjectDoesNotExistException {
    createAndStoreErrorType(errorsDAO);
    createAndStoreErrorNotification(errorsDAO);
    TaskErrorsInfo errorsInfo = service.getSpecificTaskErrorReport(TASK_ID, ERROR_TYPE, 5);
    assertEquals(1, errorsInfo.getErrors().size());
  }

  @Test
  public void shouldThrowExceptionWhenSpecificTaskErrorDoesntExistOrErrorNotificationDoesntExist() {
    assertThrows(AccessDeniedOrObjectDoesNotExistException.class,
        () -> service.getSpecificTaskErrorReport(TASK_ID, ERROR_TYPE, 5));
    createAndStoreErrorType(errorsDAO);
    assertThrows(AccessDeniedOrObjectDoesNotExistException.class,
        () -> service.getSpecificTaskErrorReport(TASK_ID, ERROR_TYPE, 5));
  }

  @Test
  public void shouldReturnGeneralTaskErrorReport() throws AccessDeniedOrObjectDoesNotExistException {
    assertEquals(0, service.getGeneralTaskErrorReport(TASK_ID, 5).getErrors().size());
    createAndStoreErrorType(errorsDAO);
    createAndStoreErrorNotification(errorsDAO);
    createAndStoreErrorType(ERROR_TYPE1, errorsDAO);
    createAndStoreErrorNotification(ERROR_TYPE1, errorsDAO);
    createAndStoreErrorType(ERROR_TYPE2, errorsDAO);
    createAndStoreErrorNotification(ERROR_TYPE2, errorsDAO);
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
    SubTaskInfo info = createAndStoreNotification(1, notificationsDAO);

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
        createAndStoreSubtaskInfoRange(300, notificationsDAO).subList(100 - 1, 200);

    List<SubTaskInfo> report = service.getDetailedTaskReport(TASK_ID, 100, 200);

    assertEquals(Lists.reverse(infoList), report);
  }


  @Test
  public void shouldReturnValidReportWhenQueryingPartOfDataInFurtherBucket() {
    List<SubTaskInfo> infoList =
        createAndStoreSubtaskInfoRange(40000, notificationsDAO).subList(21000 - 1, 22000);

    List<SubTaskInfo> report = service.getDetailedTaskReport(TASK_ID, 21000, 22000);

    assertEquals(Lists.reverse(infoList), report);
  }

  private void shouldReturnValidReportWhenQueryingBucketTemplate(int preparedReportSize, int requestTo) {
    List<SubTaskInfo> infoList = createAndStoreSubtaskInfoRange(preparedReportSize, notificationsDAO);

    List<SubTaskInfo> report = service.getDetailedTaskReport(TASK_ID, 1, requestTo);

    assertEquals(Lists.reverse(infoList), report);
  }
}