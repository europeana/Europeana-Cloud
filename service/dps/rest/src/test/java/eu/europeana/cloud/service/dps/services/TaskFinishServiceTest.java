package eu.europeana.cloud.service.dps.services;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.services.postprocessors.PostProcessingService;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import static eu.europeana.cloud.common.model.dps.TaskState.QUEUED;
import static eu.europeana.cloud.common.model.dps.TaskState.READY_FOR_POST_PROCESSING;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class TaskFinishServiceTest {


  public static final long TASK_ID = 100L;
  public static final String APPLICATION_ID = "TestApp";
  @Mock
  private TasksByStateDAO tasksByStateDAO;

  @Mock
  private PostProcessingService postProcessingService;

    @Mock
    private TaskStatusUpdater taskStatusUpdater;

    @Mock
    private CassandraTaskInfoDAO taskInfoDAO;

    private TaskFinishService service;

    @BeforeEach
    void setup() {
        service = new TaskFinishService(postProcessingService, tasksByStateDAO, taskInfoDAO, taskStatusUpdater, APPLICATION_ID);
    }

    @Test
    void shouldMarkQueuedTaskAsCompletedWhenAllRecordsProcessedAndTaskDoesNotNeedPostprocessing() {
        TaskByTaskState taskByState = TaskByTaskState.builder().id(TASK_ID).state(QUEUED).applicationId(APPLICATION_ID).build();
        when(tasksByStateDAO.findTasksByState(Collections.singletonList(QUEUED))).thenReturn(Collections.singletonList(taskByState));
        TaskInfo taskInfo = TaskInfo.builder().id(TASK_ID).state(QUEUED).expectedRecordsNumber(60).
                processedRecordsCount(30).ignoredRecordsCount(20).deletedRecordsCount(10).build();
        when(taskInfoDAO.findById(TASK_ID)).thenReturn(Optional.of(taskInfo));

        service.execute();

        verify(taskStatusUpdater).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
    }

    @Test
    void shouldMarkQueuedTaskAsReadyForPostProcessingWhenAllRecordsProcessedAndTaskDoesNotNeedPostprocessing()
            throws IOException {
        TaskByTaskState taskByState = TaskByTaskState.builder().id(TASK_ID).state(QUEUED).applicationId(APPLICATION_ID).build();
        when(tasksByStateDAO.findTasksByState(Collections.singletonList(QUEUED))).thenReturn(Collections.singletonList(taskByState));
        TaskInfo taskInfo = TaskInfo.builder().id(TASK_ID).state(QUEUED).expectedRecordsNumber(60).
                processedRecordsCount(30).ignoredRecordsCount(20).deletedRecordsCount(10).build();
        when(taskInfoDAO.findById(TASK_ID)).thenReturn(Optional.of(taskInfo));
        when(postProcessingService.needsPostprocessing(any(), any())).thenReturn(true);

        service.execute();

    verify(taskStatusUpdater).updateState(eq(TASK_ID), eq(READY_FOR_POST_PROCESSING), anyString());

  }

    @Test
    void shouldIgnoreQueuedTaskIfNotAllRecordsProcessed() {
        TaskByTaskState taskByState = TaskByTaskState.builder().id(TASK_ID).state(QUEUED).applicationId(APPLICATION_ID).build();
        when(tasksByStateDAO.findTasksByState(Collections.singletonList(QUEUED))).thenReturn(Collections.singletonList(taskByState));
        TaskInfo taskInfo = TaskInfo.builder().id(TASK_ID).state(QUEUED).expectedRecordsNumber(61).
                processedRecordsCount(30).ignoredRecordsCount(20).deletedRecordsCount(10).build();
        when(taskInfoDAO.findById(TASK_ID)).thenReturn(Optional.of(taskInfo));

        service.execute();

        verifyNoInteractions(taskStatusUpdater);
        verifyNoInteractions(postProcessingService);
  }

}