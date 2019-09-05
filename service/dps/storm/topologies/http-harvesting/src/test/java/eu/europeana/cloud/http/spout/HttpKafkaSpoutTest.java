package eu.europeana.cloud.http.spout;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.http.spout.job.TaskExecutor;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import org.apache.storm.spout.SpoutOutputCollector;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.*;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Created by Tarek on 5/4/2018.
 */

public class HttpKafkaSpoutTest {
    @Mock(name = "collector")
    private SpoutOutputCollector collector;

    @Mock(name = "cassandraTaskInfoDAO")
    private CassandraTaskInfoDAO cassandraTaskInfoDAO;

    @Mock
    private TaskStatusChecker taskStatusChecker;

    @InjectMocks
    private HttpKafkaSpout httpKafkaSpout = new HttpKafkaSpout(null);

    @Before
    public void init() throws Exception {
        MockitoAnnotations.initMocks(this);

        doNothing().when(cassandraTaskInfoDAO).updateTask(anyLong(), anyString(), anyString(), any(Date.class));
        doNothing().when(cassandraTaskInfoDAO).dropTask(anyLong(), anyString(), anyString());
        httpKafkaSpout.taskDownloader.taskQueue.clear();
        setStaticField(HttpKafkaSpout.class.getSuperclass().getDeclaredField("taskStatusChecker"), taskStatusChecker);

    }

    static void setStaticField(Field field, Object newValue) throws Exception {
        field.setAccessible(true);
        field.set(null, newValue);
    }

    @Test
    public void deactivateShouldClearTheTaskQueue() throws Exception {
        final int taskCount = 10;
        for (int i = 0; i < taskCount; i++) {
            httpKafkaSpout.taskDownloader.taskQueue.put(new DpsTask());
        }
        assertTrue(!httpKafkaSpout.taskDownloader.taskQueue.isEmpty());
        httpKafkaSpout.deactivate();
        assertTrue(httpKafkaSpout.taskDownloader.taskQueue.isEmpty());
        verify(cassandraTaskInfoDAO, atLeast(taskCount)).dropTask(anyLong(), anyString(), eq(TaskState.DROPPED.toString()));
    }
}