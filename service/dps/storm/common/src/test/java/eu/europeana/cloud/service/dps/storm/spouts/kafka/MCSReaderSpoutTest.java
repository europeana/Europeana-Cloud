package eu.europeana.cloud.service.dps.storm.spouts.kafka;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;

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

    private SpoutConfig config = new SpoutConfig(null, "", "", "");

    @InjectMocks
    private MCSReaderSpout mcsReaderSpout = new MCSReaderSpout(config, "");

    @Before
    public void init() throws Exception {
        doNothing().when(cassandraTaskInfoDAO).updateTask(anyLong(), anyString(), anyString(), any(Date.class));
        doNothing().when(cassandraTaskInfoDAO).dropTask(anyLong(), anyString(), anyString());
        setStaticField(MCSReaderSpout.class.getSuperclass().getDeclaredField("taskStatusChecker"), taskStatusChecker);
        //mcsReaderSpout.open(new HashMap(),new TopologyContext(),null);
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
            mcsReaderSpout.getTaskDownloader().getTaskQueue().put(createDpsTask());
        }
        //Thread.sleep(1000000L);
        assertTrue(!mcsReaderSpout.getTaskDownloader().getTaskQueue().isEmpty());
        mcsReaderSpout.deactivate();
        assertTrue(mcsReaderSpout.getTaskDownloader().getTaskQueue().isEmpty());
        verify(cassandraTaskInfoDAO, atLeast(taskCount)).dropTask(anyLong(), anyString(), eq(TaskState.DROPPED.toString()));
    }

    private DpsTask createDpsTask() {
        DpsTask task = new DpsTask();
        task.addDataEntry(InputDataType.DATASET_URLS, Collections.singletonList(""));
      //  task.getDataEntry()
        return task;
    }
}