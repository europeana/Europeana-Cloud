package eu.europeana.cloud.service.dps.storm.dao;

import static eu.europeana.cloud.service.dps.storm.utils.ReportTestUtils.ERROR_TYPE;
import static eu.europeana.cloud.service.dps.storm.utils.ReportTestUtils.TASK_ID;
import static eu.europeana.cloud.service.dps.storm.utils.ReportTestUtils.createAndStoreErrorType;
import static eu.europeana.cloud.service.dps.storm.utils.ReportTestUtils.createAndStoreNotification;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
import eu.europeana.cloud.service.dps.storm.utils.ReportTestUtils;
import eu.europeana.cloud.test.CassandraTestInstance;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

public class ReportDAOTest extends CassandraTestBase {

  private static final String ERROR_TYPE1 = "1c71e7b0-7633-11ed-b1fe-a7fdf50126b1";
  private static final String ERROR_TYPE2 = "1c71e7b0-7633-11ed-b1fe-a7fdf50126b2";
  private static final String ERROR_TYPE3 = "1c71e7b0-7633-11ed-b1fe-a7fdf50126b3";
  private static final String ERROR_TYPE4 = "1c71e7b0-7633-11ed-b1fe-a7fdf50126b4";
  private static final String ERROR_TYPE5 = "1c71e7b0-7633-11ed-b1fe-a7fdf50126b5";
  private ReportDAO reportDAO;
  private NotificationsDAO notificationsDAO;
  private CassandraTaskInfoDAO taskInfoDAO;
  private CassandraTaskErrorsDAO errorsDAO;

  @Before
  public void setup() {
    CassandraConnectionProvider db = CassandraConnectionProviderSingleton.getCassandraConnectionProvider(HOST,
        CassandraTestInstance.getPort(), KEYSPACE, USER,
        PASSWORD);
    reportDAO = ReportDAO.getInstance(db);
    notificationsDAO = NotificationsDAO.getInstance(db);
    taskInfoDAO = CassandraTaskInfoDAO.getInstance(db);
    errorsDAO = CassandraTaskErrorsDAO.getInstance(db);
  }


  @Test
  public void getTaskInfoRecordTest() {
    assertNull(reportDAO.getTaskInfoRecord(TASK_ID));
    ReportTestUtils.createAndStoreTaskInfo(taskInfoDAO);
    TaskInfo taskInfo = reportDAO.getTaskInfoRecord(TASK_ID);
    assertEquals(TASK_ID, taskInfo.getId());
  }

  @Test
  public void getNotificationsTest() {
    assertEquals(0, reportDAO.getNotifications(TASK_ID, 1, 5, 0).size());
    createAndStoreNotification(1, notificationsDAO);
    createAndStoreNotification(2, notificationsDAO);
    createAndStoreNotification(3, notificationsDAO);
    createAndStoreNotification(4, notificationsDAO);
    createAndStoreNotification(5, notificationsDAO);
    assertEquals(5, reportDAO.getNotifications(TASK_ID, 1, 5, 0).size());
    assertEquals(0, reportDAO.getNotifications(TASK_ID, 6, 10, 0).size());
    createAndStoreNotification(6, notificationsDAO);
    createAndStoreNotification(6, notificationsDAO);
    assertEquals(1, reportDAO.getNotifications(TASK_ID, 6, 10, 0).size());
  }

  @Test
  public void getErrorTypesTest() {
    assertEquals(0, reportDAO.getErrorTypes(TASK_ID).size());
    createAndStoreErrorType(ERROR_TYPE1, errorsDAO);
    assertEquals(1, reportDAO.getErrorTypes(TASK_ID).size());
    createAndStoreErrorType(ERROR_TYPE2, errorsDAO);
    createAndStoreErrorType(ERROR_TYPE3, errorsDAO);
    createAndStoreErrorType(ERROR_TYPE4, errorsDAO);
    createAndStoreErrorType(ERROR_TYPE5, errorsDAO);
    assertEquals(5, reportDAO.getErrorTypes(TASK_ID).size());
    createAndStoreErrorType(ERROR_TYPE5, errorsDAO);
    assertEquals(5, reportDAO.getErrorTypes(TASK_ID).size());
  }

  @Test
  public void getErrorNotificationsTest() {
    assertEquals(0, reportDAO.getErrorNotifications(TASK_ID, UUID.fromString(ERROR_TYPE), 1).size());
    ReportTestUtils.createAndStoreErrorNotification(ERROR_TYPE, errorsDAO);
    assertEquals(1, reportDAO.getErrorNotifications(TASK_ID, UUID.fromString(ERROR_TYPE), 1).size());
    ReportTestUtils.createAndStoreErrorNotification(ERROR_TYPE, errorsDAO);
    assertEquals(1, reportDAO.getErrorNotifications(TASK_ID, UUID.fromString(ERROR_TYPE), 5).size());
  }

  @Test
  public void getErrorTypeTest() {
    assertNull(reportDAO.getErrorType(TASK_ID, UUID.fromString(ERROR_TYPE)));
    ReportTestUtils.createAndStoreErrorType(ERROR_TYPE, errorsDAO);
    assertNotNull(reportDAO.getErrorType(TASK_ID, UUID.fromString(ERROR_TYPE)));
  }
}