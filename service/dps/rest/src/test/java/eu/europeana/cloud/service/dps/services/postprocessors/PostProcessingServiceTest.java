package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskDroppedException;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class PostProcessingServiceTest {

  private final long TASK_ID_1 = 1L;
  private final long TASK_ID_2 = 2L;
  private final TaskInfo TASK_INFO_1 = TaskInfo.builder().build();
  private final TaskByTaskState TASK_BY_TASK_STATE_1
          = TaskByTaskState.builder().id(TASK_ID_1).topologyName(TopologiesNames.HTTP_TOPOLOGY).build();
  private final TaskByTaskState TASK_BY_TASK_STATE_2
          = TaskByTaskState.builder().id(TASK_ID_2).topologyName("UNKNOWN_TOPOLOGY").build();

  private final String TASK_DETAILS_PATTERN = "{\"taskId\":%s}";


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

  @BeforeEach
  void initTest() {
    initTaskInfoDAOMock();
    initPostProcessorFactory();
  }


  @Test
  void shouldExecutePostprocessor() throws TaskDroppedException {
    postProcessingService.postProcess(TASK_BY_TASK_STATE_1);
    verify(taskPostProcessor).execute(any(), any());
  }

  @Test
  void shouldNotExecuteForUnknownTopology() throws TaskDroppedException {
    postProcessingService.postProcess(TASK_BY_TASK_STATE_2);
    verify(taskPostProcessor, never()).execute(any(), any());
  }

  @Test
  void shouldNeedsPostProcessingReturnFalseIfFactoryNotFoundForGivenTopology() throws IOException {

    boolean result = postProcessingService.needsPostprocessing(TASK_BY_TASK_STATE_2, TASK_INFO_1);

    assertFalse(result);
  }

  @Test
  void shouldNeedsPostProcessingReturnFalseIfPostProcessorReturnFalse() throws IOException {

    boolean result = postProcessingService.needsPostprocessing(TASK_BY_TASK_STATE_1, TASK_INFO_1);

    assertFalse(result);
  }

  @Test
  void shouldNeedsPostProcessingReturnTrueIfPostProcessorReturnTrue() throws IOException {
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
