package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.oaipmh.HarvesterException;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Matchers.eq;

@RunWith(MockitoJUnitRunner.class)
public class HarvestExecutorTest {
    private static final int CASE = 0;
    private static final long TASK_ID = 1234567890L;
    private static final String OAI_TOPOLOGY_NAME = "OAI_TOPOLOGY";
    private static final String TOPIC_NAME = "topic";
    private final int HARVESTS_INDEX = 0;

    @Mock
    private RecordExecutionSubmitService recordSubmitService;

    @Mock
    private ProcessedRecordsDAO processedRecordsDAO;

    @Mock
    private TaskStatusChecker taskStatusChecker;

    @Spy
    @InjectMocks
    private HarvestsExecutor harvestsExecutor;

    private List<Harvest> harvestList;

    private static final String[][] DATA = new String[][]{
            {"http://islandskort.is/oai", "edm", ""},
            {"http://baekur.is/oai", "edm", ""},
            {"http://test117.ait.co.at/oai-provider-edm/oai", "edm", ""}
    };

    @Before
    public void setupCases() {
        String endPoint = DATA[CASE][0];
        String schema = DATA[CASE][1];
        String set = DATA[CASE][2];

        Harvest harvest = Harvest.builder()
                .url(endPoint)
                .oaiSetSpec(set != null && set.isEmpty() ?  null : set)
                .metadataPrefix(schema)
                .build();

        harvestList = new ArrayList<>();
        harvestList.add(HARVESTS_INDEX, harvest);
    }

    @Test
    public void shouldDropTaskWhenItKilled() throws HarvesterException {
        DpsTask dpsTask = new DpsTask();
        dpsTask.setTaskId(TASK_ID);

        Mockito.when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(true);

        HarvestResult harvestResult = harvestsExecutor.execute(OAI_TOPOLOGY_NAME, harvestList, dpsTask, TOPIC_NAME);

        Assert.assertEquals(TaskState.DROPPED, harvestResult.getTaskState());
    }

    @Test
    public void shouldCallAllNecessaryRoutines() throws HarvesterException {
        DpsTask dpsTask = new DpsTask();
        dpsTask.setTaskId(TASK_ID);

        Mockito.when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);

        int count = harvestsExecutor.execute(OAI_TOPOLOGY_NAME, harvestList, dpsTask, TOPIC_NAME).getResultCounter();

        Mockito.verify(harvestsExecutor, Mockito.times(count)).convertToDpsRecord(Matchers.any(OAIHeader.class), eq(harvestList.get(HARVESTS_INDEX)), eq(dpsTask));
        Mockito.verify(harvestsExecutor, Mockito.times(count)).sentMessage(Matchers.any(DpsRecord.class), Mockito.anyString());
        Mockito.verify(harvestsExecutor, Mockito.times(count)).updateRecordStatus(Matchers.any(DpsRecord.class), Mockito.anyString());
        Mockito.verify(harvestsExecutor, Mockito.times(count)).logProgressFor(eq(harvestList.get(HARVESTS_INDEX)), Mockito.anyInt());
    }
}
