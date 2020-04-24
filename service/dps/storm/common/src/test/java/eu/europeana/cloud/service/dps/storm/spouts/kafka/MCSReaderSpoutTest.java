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
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.Date;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Created by Tarek on 5/21/2018.
 */

@RunWith(MockitoJUnitRunner.class)
public class MCSReaderSpoutTest {
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
        doNothing().when(cassandraTaskInfoDAO).setTaskDropped(anyLong(), anyString());
        setStaticField(MCSReaderSpout.class.getSuperclass().getDeclaredField("taskStatusChecker"), taskStatusChecker);
        mcsReaderSpout.getTaskDownloader().getTaskQueue().clear();
    }

    private static void setStaticField(Field field, Object newValue) throws Exception {
        field.setAccessible(true);
        field.set(null, newValue);
    }

    @Test
    public void deactivateShouldClearTheTaskQueue() throws Exception {
        final int taskCount = 10;
        for (int i = 0; i < taskCount; i++) {
            mcsReaderSpout.getTaskDownloader().getTaskQueue().put(new DpsTask());
        }
        assertTrue(!mcsReaderSpout.getTaskDownloader().getTaskQueue().isEmpty());
        mcsReaderSpout.deactivate();
        assertTrue(mcsReaderSpout.getTaskDownloader().getTaskQueue().isEmpty());
        verify(cassandraTaskInfoDAO, atLeast(taskCount)).setTaskDropped(anyLong(), anyString());
    }
}