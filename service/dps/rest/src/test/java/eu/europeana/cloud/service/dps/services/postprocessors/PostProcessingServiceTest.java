package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Optional;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class PostProcessingServiceTest {

    private long TASK_ID_1 = 1l;
    private long TASK_ID_2 = 2l;
    private TaskInfo TASK_INFO_1 = new  TaskInfo();
    private TaskByTaskState TASK_BY_TASK_STATE_1
            = TaskByTaskState.builder().id(TASK_ID_1).topologyName(TopologiesNames.HTTP_TOPOLOGY).build();
    private TaskByTaskState TASK_BY_TASK_STATE_2
            = TaskByTaskState.builder().id(TASK_ID_2).topologyName("UNKNOWN_TOPOLOGY").build();

    private String TASK_DETAILS_PATTERN = "{\"inputData\":{\"DATASET_URLS\":[\"http://a.b.c/d/e/f\"]}, \"taskId\":%s}";


    @Mock
    private CassandraTaskInfoDAO taskInfoDAO;

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
    public void shouldExecutePostprocessor() throws Exception {
        postProcessingService.postProcess(TASK_BY_TASK_STATE_1);
        verify(taskPostProcessor).execute(any());
    }

    @Test
    public void shouldNotExecuteForUnknownTopology() {
        postProcessingService.postProcess(TASK_BY_TASK_STATE_2);
        verify(taskPostProcessor, never()).execute(any());
    }

    private void initTaskInfoDAOMock() {
        TASK_INFO_1.setId(TASK_ID_1);
        TASK_INFO_1.setTaskDefinition(String.format(TASK_DETAILS_PATTERN, TASK_ID_1));

        when(taskInfoDAO.findById(TASK_ID_1)).thenReturn(Optional.ofNullable(TASK_INFO_1));
    }

    private void initPostProcessorFactory() {
        when(postProcessorFactory.getPostProcessor(TASK_BY_TASK_STATE_1)).thenReturn(taskPostProcessor);
        when(postProcessorFactory.getPostProcessor(TASK_BY_TASK_STATE_2)).thenThrow(PostProcessingException.class);
    }
}
