package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import org.apache.storm.spout.SpoutOutputCollector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.Date;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

/**
 * Created by Tarek on 5/21/2018.
 */

@RunWith(PowerMockRunner.class)
@PrepareForTest(MCSReaderSpout.class)
public class MCSReaderSpoutTest {
    public static final String DATASET_URL = "http://localhost:8080/mcs/data-providers/testDataProvider/data-sets/dataSet";
    @Mock(name = "collector")
    private SpoutOutputCollector collector;

    @Mock(name = "cassandraTaskInfoDAO")
    private CassandraTaskInfoDAO cassandraTaskInfoDAO;

    @Mock
    private TaskStatusChecker taskStatusChecker;


    @InjectMocks
    private MCSReaderSpout mcsReaderSpout = new MCSReaderSpout(null);

    @Before
    public void init() throws Exception {
        doNothing().when(cassandraTaskInfoDAO).updateTask(anyLong(), anyString(), anyString(), any(Date.class));
        doNothing().when(cassandraTaskInfoDAO).dropTask(anyLong(), anyString(), anyString());
        setStaticField(MCSReaderSpout.class.getSuperclass().getDeclaredField("taskStatusChecker"), taskStatusChecker);
        mcsReaderSpout.taskDownloader.taskQueue.clear();
    }

    private static void setStaticField(Field field, Object newValue) throws Exception {
        field.setAccessible(true);
        field.set(null, newValue);
    }

    @Test
    public void deactivateShouldClearTheTaskQueue() throws Exception {
        final int taskCount = 10;
        for (int i = 0; i < taskCount; i++) {
            mcsReaderSpout.taskDownloader.taskQueue.put(new DpsTask());
        }
        assertTrue(!mcsReaderSpout.taskDownloader.taskQueue.isEmpty());
        mcsReaderSpout.deactivate();
        assertTrue(mcsReaderSpout.taskDownloader.taskQueue.isEmpty());
        verify(cassandraTaskInfoDAO, atLeast(taskCount)).dropTask(anyLong(), anyString(), eq(TaskState.DROPPED.toString()));
    }
}