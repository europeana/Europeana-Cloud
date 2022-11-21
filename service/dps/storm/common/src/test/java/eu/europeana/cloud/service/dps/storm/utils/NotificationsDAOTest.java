package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.dao.NotificationsDAO;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

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

  private void insertNotifications(int count) {
    for (int i = 1; i <= count; i++) {
      subtaskInfoDao.insert(i, 111, "topologyName", "resource" + i, TaskState.QUEUED.toString(),
          "infoTxt", Map.of(NotificationsDAO.STATE_DESCRIPTION_KEY, "additionalInformation"), "resultResource" + i);
    }
  }
}