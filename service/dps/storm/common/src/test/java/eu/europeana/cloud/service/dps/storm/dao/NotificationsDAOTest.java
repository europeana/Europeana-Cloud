package eu.europeana.cloud.service.dps.storm.dao;

import static eu.europeana.cloud.service.dps.storm.utils.ServiceAndDAOTestUtils.createAndStoreNotification;
import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertEquals;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
import eu.europeana.cloud.test.CassandraTestInstance;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class NotificationsDAOTest extends CassandraTestBase {

  private static final long TASK_ID = 111;
  private NotificationsDAO subtaskInfoDao;

  @Before
  public void setup() {
    CassandraConnectionProvider db = new CassandraConnectionProvider(HOST, CassandraTestInstance.getPort(), KEYSPACE, USER,
        PASSWORD);
    subtaskInfoDao = NotificationsDAO.getInstance(db);
  }

  @Test
  public void shouldReturnOWhenExecuteGetProcessedFilesCountOnEmptyNotifications() {
    int result = subtaskInfoDao.getProcessedFilesCount(TASK_ID);

    assertEquals(0, result);
  }

  @Test
  public void shouldReturn1WhenExecuteGetProcessedFilesCountOnOneRecord() {
    shouldReturnValidNumberWhenExecuteGetProcessedFilesCountTemplate(1);
  }

  @Test
  public void shouldReturnValidNumberWhenExecuteGetProcessedFilesCountOnOneBucketData() {
    shouldReturnValidNumberWhenExecuteGetProcessedFilesCountTemplate(100);
  }

  @Test
  public void shouldReturnValidNumberWhenExecuteGetProcessedFilesCountManyBucketsData() {
    shouldReturnValidNumberWhenExecuteGetProcessedFilesCountTemplate(34567);
  }

  private void shouldReturnValidNumberWhenExecuteGetProcessedFilesCountTemplate(int count) {
    insertNotifications(count);

    int result = subtaskInfoDao.getProcessedFilesCount(TASK_ID);

    assertEquals(count, result);
  }

  @Test
  public void shouldReturnEmptyListWhenNoNotificationsRecords() {
    assertEquals(0, subtaskInfoDao.getNotificationsFromGivenBucketAndWithinGivenResourceNumRange(TASK_ID, 1, 5, 0).size());
  }

  @Test
  public void shouldReturnAllNotificationsInContinuousRange() {
    createAndStoreNotification(1, subtaskInfoDao);
    createAndStoreNotification(2, subtaskInfoDao);
    createAndStoreNotification(3, subtaskInfoDao);
    createAndStoreNotification(4, subtaskInfoDao);
    createAndStoreNotification(5, subtaskInfoDao);
    assertEquals(5, subtaskInfoDao.getNotificationsFromGivenBucketAndWithinGivenResourceNumRange(TASK_ID, 1, 5, 0).size());
  }

  @Test
  public void shouldNotReturnNotificationsFromOutsideOfRange() {
    createAndStoreNotification(1, subtaskInfoDao);
    createAndStoreNotification(2, subtaskInfoDao);
    createAndStoreNotification(3, subtaskInfoDao);
    createAndStoreNotification(4, subtaskInfoDao);
    createAndStoreNotification(5, subtaskInfoDao);
    assertEquals(0, subtaskInfoDao.getNotificationsFromGivenBucketAndWithinGivenResourceNumRange(TASK_ID, 6, 10, 0).size());
    assertEquals(0, subtaskInfoDao.getNotificationsFromGivenBucketAndWithinGivenResourceNumRange(TASK_ID, 6, 6, 0).size());
    assertEquals(0, subtaskInfoDao.getNotificationsFromGivenBucketAndWithinGivenResourceNumRange(TASK_ID, 0, 0, 0).size());
  }

  private void insertNotifications(int count) {
    for (int i = 1; i <= count; i++) {
      subtaskInfoDao.insert(i, 111, "topologyName", "resource" + i, TaskState.QUEUED.toString(),
          "infoTxt", Map.of(NotificationsDAO.STATE_DESCRIPTION_KEY, "additionalInformation"), "resultResource" + i);
    }
  }
}