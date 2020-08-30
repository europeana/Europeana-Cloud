package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.config.HarvestsExecutorContext;
import eu.europeana.cloud.service.dps.oaipmh.HarvesterException;
import eu.europeana.cloud.service.dps.storm.spouts.kafka.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.spy;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes={HarvestsExecutorContext.class, HarvestsExecutor.class})
public class HarvestExecutorTest {
    private final static int CASE = 0;
    private final static long TASK_ID = 1234567890L;
    private final static String OAI_TOPOLOGY_NAME = "OAI_TOPOLOGY";
    private final static String TOPIC_NAME = "topic";
    private final int HARVESTS_INDEX = 0;

    @Autowired
    private TaskStatusChecker taskStatusChecker;

    @Autowired
    private HarvestsExecutor harvestsExecutor;

    private List<Harvest> harvestList;

    private final static String[][] DATA = new String[][]{
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

        HarvestResult harvestResult = harvestsExecutor.execute(harvestList,
                SubmitTaskParameters.builder()
                        .task(dpsTask)
                        .topicName(TOPIC_NAME)
                        .topologyName(OAI_TOPOLOGY_NAME)
                        .build());

        Assert.assertEquals(TaskState.DROPPED, harvestResult.getTaskState());
    }

    @Test
    public void shouldCallAllNecessaryRoutines() throws HarvesterException {
        DpsTask dpsTask = new DpsTask();
        dpsTask.setTaskId(TASK_ID);
        dpsTask.addDataEntry(InputDataType.REPOSITORY_URLS, Arrays.asList(""));

        Mockito.when(taskStatusChecker.hasKillFlag(TASK_ID)).thenReturn(false);

        HarvestsExecutor spiedHarvestsExecutor = spy(harvestsExecutor);
        int count = spiedHarvestsExecutor.execute(harvestList,
                SubmitTaskParameters.builder()
                        .task(dpsTask)
                        .topicName(TOPIC_NAME)
                        .topologyName(OAI_TOPOLOGY_NAME)
                        .build()).getResultCounter();

        Mockito.verify(spiedHarvestsExecutor, Mockito.times(count)).convertToDpsRecord(Matchers.any(OAIHeader.class), eq(harvestList.get(HARVESTS_INDEX)), eq(dpsTask));
        Mockito.verify(spiedHarvestsExecutor, Mockito.times(count)).sendMessage(Matchers.any(DpsRecord.class), Mockito.anyString());
        Mockito.verify(spiedHarvestsExecutor, Mockito.times(count)).updateRecordStatus(Matchers.any(DpsRecord.class), Mockito.anyString());
        Mockito.verify(spiedHarvestsExecutor, Mockito.times(count)).logProgressFor(eq(harvestList.get(HARVESTS_INDEX)), Mockito.anyInt());
    }
}
