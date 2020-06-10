package eu.europeana.cloud.service.dps.storm.spout;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.test.TestHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.spout.SpoutOutputCollector;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static eu.europeana.cloud.service.dps.test.TestConstants.DATA_PROVIDER;
import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE_VERSION_URL;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Created by Tarek on 9/18/2018.
 */
public class QueueFillerTest {

    @Mock(name = "collector")
    private SpoutOutputCollector collector;

    @Mock(name = "cassandraTaskInfoDAO")
    private CassandraTaskInfoDAO cassandraTaskInfoDAO;
    private TestHelper testHelper = new TestHelper();

    @Mock
    private TaskStatusChecker taskStatusChecker;

    @InjectMocks
    private QueueFiller queueFiller = new QueueFiller(taskStatusChecker, collector, new ArrayBlockingQueue<StormTaskTuple>(10));

    private static final String BASE_URL = "http://MCS.com";

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testAddingToQueueSuccessfully() throws Exception {
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(false);
        Representation representation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date());
        assertEquals(0, queueFiller.tuplesWithFileUrls.size());
        for (int i = 0; i < 10; i++)
            queueFiller.addTupleToQueue(new StormTaskTuple(), new FileServiceClient(BASE_URL), representation);
        assertEquals(10, queueFiller.tuplesWithFileUrls.size());
    }


    @Test
    public void testKillingTheTaskEffectOnQueue() throws Exception {
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(false, false, false, true);
        Representation representation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date());
        assertEquals(0, queueFiller.tuplesWithFileUrls.size());
        for (int i = 0; i < 10; i++)
            queueFiller.addTupleToQueue(new StormTaskTuple(), new FileServiceClient(BASE_URL), representation);
        assertEquals(3, queueFiller.tuplesWithFileUrls.size());
    }

    @Test
    public void shouldEmitErrorsInCaseOfExceptionWhileGettingTheFiles() throws Exception {
        when(taskStatusChecker.hasKillFlag(anyLong())).thenReturn(false);
        Representation representation = testHelper.prepareRepresentation(SOURCE + CLOUD_ID, SOURCE + REPRESENTATION_NAME, SOURCE + VERSION, SOURCE_VERSION_URL, DATA_PROVIDER, false, new Date());
        FileServiceClient fileServiceClient = Mockito.mock(FileServiceClient.class);
        doThrow(MCSException.class).when(fileServiceClient).getFile(anyString(),anyString(),anyString(),anyString());
        assertEquals(0, queueFiller.tuplesWithFileUrls.size());
        for (int i = 0; i < 10; i++)
            queueFiller.addTupleToQueue(new StormTaskTuple(), fileServiceClient, representation);
        assertEquals(0, queueFiller.tuplesWithFileUrls.size());
        verify(collector, times(10)).emit(Matchers.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), anyListOf(Object.class));

    }

}