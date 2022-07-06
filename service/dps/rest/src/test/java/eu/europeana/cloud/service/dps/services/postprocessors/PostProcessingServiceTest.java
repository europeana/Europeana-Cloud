package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class PostProcessingServiceTest {

    private final long TASK_ID_1 = 1L;
    private final long TASK_ID_2 = 2L;
    private final TaskInfo TASK_INFO_1 = TaskInfo.builder().build();
    private final TaskByTaskState TASK_BY_TASK_STATE_1
            = TaskByTaskState.builder().id(TASK_ID_1).topologyName(TopologiesNames.HTTP_TOPOLOGY).build();
    private final TaskByTaskState TASK_BY_TASK_STATE_2
            = TaskByTaskState.builder().id(TASK_ID_2).topologyName("UNKNOWN_TOPOLOGY").build();

    private final String TASK_DETAILS_PATTERN = "{\"inputData\":{\"DATASET_URLS\":[\"http://a.b.c/d/e/f\"]}, \"taskId\":%s}";


    @Mock
    private CassandraTaskInfoDAO taskInfoDAO;

    @Mock
    private TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;

    @Mock
    private TasksByStateDAO tasksByStateDAO;

    @Mock
    private TaskStatusUpdater taskStatusUpdater;

    @Mock
    private PostProcessorFactory postProcessorFactory;

    @Mock
    private TaskPostProcessor taskPostProcessor;

    @InjectMocks
    private PostProcessingService postProcessingService;

    @Before
    public void initTest() {
        initTaskInfoDAOMock();
        initPostProcessorFactory();
    }


    @Test
    public void shouldExecutePostprocessor() {
        postProcessingService.postProcess(TASK_BY_TASK_STATE_1);
        verify(taskPostProcessor).execute(any(), any());
    }

    @Test
    public void shouldNotExecuteForUnknownTopology() {
        postProcessingService.postProcess(TASK_BY_TASK_STATE_2);
        verify(taskPostProcessor, never()).execute(any(), any());
    }

    @Test
    public void shouldNeedsPostProcessingReturnFalseIfFactoryNotFoundForGivenTopology() throws IOException {

        boolean result = postProcessingService.needsPostprocessing(TASK_BY_TASK_STATE_2, TASK_INFO_1);

        assertFalse(result);
    }

    @Test
    public void shouldNeedsPostProcessingReturnFalseIfPostProcessorReturnFalse() throws IOException {

        boolean result = postProcessingService.needsPostprocessing(TASK_BY_TASK_STATE_1, TASK_INFO_1);

        assertFalse(result);
    }

    @Test
    public void shouldNeedsPostProcessingReturnTrueIfPostProcessorReturnTrue() throws IOException {
        when(taskPostProcessor.needsPostProcessing(any())).thenReturn(true);

        boolean result = postProcessingService.needsPostprocessing(TASK_BY_TASK_STATE_1, TASK_INFO_1);

        assertTrue(result);
    }

    private void initTaskInfoDAOMock() {
        TASK_INFO_1.setId(TASK_ID_1);
        TASK_INFO_1.setDefinition(String.format(TASK_DETAILS_PATTERN, TASK_ID_1));

        when(taskInfoDAO.findById(TASK_ID_1)).thenReturn(Optional.of(TASK_INFO_1));
    }

    private void initPostProcessorFactory() {
        when(postProcessorFactory.getPostProcessor(TASK_BY_TASK_STATE_1)).thenReturn(taskPostProcessor);
        when(postProcessorFactory.findPostProcessor(TASK_BY_TASK_STATE_1)).thenReturn(Optional.of(taskPostProcessor));
        when(postProcessorFactory.getPostProcessor(TASK_BY_TASK_STATE_2)).thenThrow(PostProcessingException.class);
        when(postProcessorFactory.findPostProcessor(TASK_BY_TASK_STATE_2)).thenReturn(Optional.empty());
    }
}
