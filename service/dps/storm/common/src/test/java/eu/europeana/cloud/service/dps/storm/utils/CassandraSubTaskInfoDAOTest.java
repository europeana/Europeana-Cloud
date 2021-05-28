package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.dao.CassandraSubTaskInfoDAO;
import eu.europeana.cloud.test.CassandraTestInstance;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class CassandraSubTaskInfoDAOTest extends CassandraTestBase {

    private static final long TASK_ID = 111;
    private CassandraSubTaskInfoDAO subtaskInfoDao;

    @Before
    public void setup() {
        CassandraConnectionProvider db = new CassandraConnectionProvider(HOST, CassandraTestInstance.getPort(), KEYSPACE, USER, PASSWORD);
        subtaskInfoDao = CassandraSubTaskInfoDAO.getInstance(db);
    }

    @Test
    public void shouldReturnOWhenExecuteGetProcessedFilesCountOnEmptyNotifications(){
        int result = subtaskInfoDao.getProcessedFilesCount(TASK_ID);

        assertEquals(0,result);
    }

    @Test
    public void shouldReturn1WhenExecuteGetProcessedFilesCountOnOneRecord(){
        insertNotifications(1);

        int result = subtaskInfoDao.getProcessedFilesCount(TASK_ID);

        assertEquals(1,result);
    }

    @Test
    public void shouldReturnValidNumberWhenExecuteGetProcessedFilesCountOnOneBucketData(){
        insertNotifications(100);

        int result = subtaskInfoDao.getProcessedFilesCount(TASK_ID);

        assertEquals(100,result);
    }

    @Test
    public void shouldReturnValidNumberWhenExecuteGetProcessedFilesCountManyBucketsData(){
        insertNotifications(34567);

        int result = subtaskInfoDao.getProcessedFilesCount(TASK_ID);

        assertEquals(34567,result);
    }

    private void insertNotifications(int count) {
        for(int i=1;i<=count;i++) {
            subtaskInfoDao.insert(i, 111, "topologyName", "resource" + i, TaskState.QUEUED.toString(), "infoTxt", "additionalInformations", "resultResource" + i);
        }
    }


}