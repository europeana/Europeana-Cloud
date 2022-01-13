package eu.europeana.cloud.service.dps.services.postprocessors;


import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class PostProcessorFactoryTest {

    private HarvestingPostProcessor harvestingPostProcessor;
    private IndexingPostProcessor indexingPostProcessor;
    private PostProcessorFactory postProcessorFactory;

    @Before
    public void initFactory() {
        harvestingPostProcessor = new HarvestingPostProcessor (
                Mockito.mock(HarvestedRecordsDAO.class),
                Mockito.mock(ProcessedRecordsDAO.class),
                Mockito.mock(RecordServiceClient.class),
                Mockito.mock(RevisionServiceClient.class),
                Mockito.mock(UISClient.class),
                Mockito.mock(DataSetServiceClient.class),
                Mockito.mock(TaskStatusUpdater.class),
                Mockito.mock(TaskStatusChecker.class)
        );
        indexingPostProcessor = new IndexingPostProcessor (
                Mockito.mock(TaskStatusUpdater.class),
                Mockito.mock(HarvestedRecordsDAO.class),
                Mockito.mock(TaskStatusChecker.class)
        );

        postProcessorFactory = new PostProcessorFactory(Arrays.asList(harvestingPostProcessor, indexingPostProcessor));
    }

    @Test
    public void shouldChooseValidPostProcessor() {
        Set<String> topologiesForIndexing =  indexingPostProcessor.getProcessedTopologies();
        topologiesForIndexing.forEach(topologyName -> shouldReturnApropriatePostProcessor(indexingPostProcessor, topologyName));

        Set<String> topologiesForHarvesting =  harvestingPostProcessor.getProcessedTopologies();
        topologiesForHarvesting.forEach(topologyName -> shouldReturnApropriatePostProcessor(harvestingPostProcessor, topologyName));
    }

    @Test(expected = PostProcessingException.class)
    public void shouldFailForUnknownTopologyProcessor() {
        var taskByTaskState = TaskByTaskState.builder().topologyName("STRANGE_TOPOLOGY").build();
        postProcessorFactory.getPostProcessor(taskByTaskState);
    }

    private void shouldReturnApropriatePostProcessor(TaskPostProcessor expectedPostProcessor, String topologyName) {
        var taskByTaskState = TaskByTaskState.builder().topologyName(topologyName).build();
        var postProcessorFromFactory = postProcessorFactory.getPostProcessor(taskByTaskState);

        Assert.assertSame(expectedPostProcessor, postProcessorFromFactory);
    }

}
