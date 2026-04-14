package eu.europeana.cloud.service.dps.storm.dao;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.EngineTaskState;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTestBase;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static eu.europeana.cloud.service.dps.storm.utils.ServiceAndDAOTestUtils.createAndStoreNotification;
import static org.junit.jupiter.api.Assertions.assertEquals;

class NotificationsDAOTest extends CassandraTestBase {

    private static final long TASK_ID = 111;
    private NotificationsDAO subtaskInfoDao;

    @BeforeEach
    void setup() {
        CassandraConnectionProvider db = new CassandraConnectionProvider(HOST, CassandraTestInstance.getPort(), KEYSPACE, USER,
                PASSWORD);
        subtaskInfoDao = NotificationsDAO.getInstance(db);
    }

    @Test
    void shouldReturnOWhenExecuteGetProcessedFilesCountOnEmptyNotifications() {
        int result = subtaskInfoDao.getProcessedFilesCount(TASK_ID);

        assertEquals(0, result);
    }

    @Test
    void shouldReturn1WhenExecuteGetProcessedFilesCountOnOneRecord() {
        shouldReturnValidNumberWhenExecuteGetProcessedFilesCountTemplate(1);
    }

    @Test
    void shouldReturnValidNumberWhenExecuteGetProcessedFilesCountOnOneBucketData() {
        shouldReturnValidNumberWhenExecuteGetProcessedFilesCountTemplate(100);
    }

    @Test
    void shouldReturnValidNumberWhenExecuteGetProcessedFilesCountManyBucketsData() {
        shouldReturnValidNumberWhenExecuteGetProcessedFilesCountTemplate(34567);
    }

  private void shouldReturnValidNumberWhenExecuteGetProcessedFilesCountTemplate(int count) {
    insertNotifications(count);

    int result = subtaskInfoDao.getProcessedFilesCount(TASK_ID);

    assertEquals(count, result);
  }

    @Test
    void shouldReturnEmptyListWhenNoNotificationsRecords() {
        assertEquals(0, subtaskInfoDao.getNotificationsFromGivenBucketAndWithinGivenResourceNumRange(TASK_ID, 1, 5, 0).size());
    }

    @Test
    void shouldReturnAllNotificationsInContinuousRange() {
        createAndStoreNotification(1, subtaskInfoDao);
        createAndStoreNotification(2, subtaskInfoDao);
        createAndStoreNotification(3, subtaskInfoDao);
        createAndStoreNotification(4, subtaskInfoDao);
        createAndStoreNotification(5, subtaskInfoDao);
        assertEquals(5, subtaskInfoDao.getNotificationsFromGivenBucketAndWithinGivenResourceNumRange(TASK_ID, 1, 5, 0).size());
    }

    @Test
    void shouldNotReturnNotificationsFromOutsideOfRange() {
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
        subtaskInfoDao.insert(i, 111, "topologyName", "resource" + i, EngineTaskState.QUEUED.toString(),
          "infoTxt", Map.of(NotificationsDAO.STATE_DESCRIPTION_KEY, "additionalInformation"), "resultResource" + i);
    }
  }
}