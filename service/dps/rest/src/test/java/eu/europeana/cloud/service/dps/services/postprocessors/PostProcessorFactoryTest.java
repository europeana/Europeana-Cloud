package eu.europeana.cloud.service.dps.services.postprocessors;


import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class PostProcessorFactoryTest {

    private HarvestingPostProcessor harvestingPostProcessor;
    private IndexingPostProcessor indexingPostProcessor;
    private PostProcessorFactory postProcessorFactory;

    @Before
    public void initFactory() {
        Map<String, TaskPostProcessor> postProcessors = new HashMap<>();

        harvestingPostProcessor = new HarvestingPostProcessor (
                Mockito.mock(HarvestedRecordsDAO.class),
                Mockito.mock(ProcessedRecordsDAO.class),
                Mockito.mock(RecordServiceClient.class),
                Mockito.mock(RevisionServiceClient.class),
                Mockito.mock(UISClient.class),
                Mockito.mock(DataSetServiceClient.class),
                Mockito.mock(TaskStatusUpdater.class)
        );
        harvestingPostProcessor.getProcessedTopologies().forEach(
                topologyName -> postProcessors.put(topologyName, harvestingPostProcessor));

        indexingPostProcessor = new IndexingPostProcessor (
                Mockito.mock(TaskStatusUpdater.class),
                Mockito.mock(HarvestedRecordsDAO.class)
        );
        indexingPostProcessor.getProcessedTopologies().forEach(
                topologyName -> postProcessors.put(topologyName, indexingPostProcessor));

        postProcessorFactory = new PostProcessorFactory(postProcessors);
    }

    @Test
    public void shouldChooseValidPostProcessor() {
        Set<String> topologesForIndexing =  indexingPostProcessor.getProcessedTopologies();
        topologesForIndexing.forEach(topologName -> shouldReturnApropriatePostProcessor(indexingPostProcessor, topologName));

        Set<String> topologesForHarvesting =  harvestingPostProcessor.getProcessedTopologies();
        topologesForHarvesting.forEach(topologName -> shouldReturnApropriatePostProcessor(harvestingPostProcessor, topologName));
    }

    private void shouldReturnApropriatePostProcessor(TaskPostProcessor expectedPostProcessor, String topologyName) {
        var taskByTaskState = TaskByTaskState.builder().topologyName(topologyName).build();
        var postProcessorFromFactory = postProcessorFactory.getPostProcessor(taskByTaskState);

        postProcessorFromFactory.ifPresentOrElse(
                postProcessor -> Assert.assertEquals(expectedPostProcessor, postProcessor),
                () -> {Assert.fail();});
    }

}
