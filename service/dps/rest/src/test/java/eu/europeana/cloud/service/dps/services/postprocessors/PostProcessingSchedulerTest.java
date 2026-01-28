package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;

import static eu.europeana.cloud.common.model.dps.TaskState.IN_POST_PROCESSING;
import static eu.europeana.cloud.common.model.dps.TaskState.READY_FOR_POST_PROCESSING;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class PostProcessingSchedulerTest {


  public static final long TASK_ID = 100L;
  public static final String APPLICATION_ID = "TestApp";
  @Mock
  private TasksByStateDAO tasksByStateDAO;

    @Mock
    private PostProcessingService postProcessingService;

    @Mock
    private TaskStatusUpdater taskStatusUpdater;

    private PostProcessingScheduler scheduler;

    @BeforeEach
    void setup() {
        scheduler = new PostProcessingScheduler(postProcessingService, tasksByStateDAO, taskStatusUpdater, APPLICATION_ID);
    }

    //Activities on start - init() method
    @Test
    void shouldResetStateOfInPostProcessingTasksOnInit() {
        TaskByTaskState task = TaskByTaskState.builder().id(TASK_ID).state(IN_POST_PROCESSING)
                .applicationId(APPLICATION_ID).build();
        when(tasksByStateDAO.findTasksByState(Collections.singletonList(IN_POST_PROCESSING)))
                .thenReturn(Collections.singletonList(task));

        scheduler.init();

        verify(taskStatusUpdater).updateState(eq(TASK_ID), eq(TaskState.READY_FOR_POST_PROCESSING), anyString());
    }

    //Scheduled activities - execute method()
    @Test
    void shouldSendReadyTasksToPostProcessing() {
        TaskByTaskState taskByState = TaskByTaskState.builder().id(TASK_ID).state(READY_FOR_POST_PROCESSING)
                .applicationId(APPLICATION_ID).build();
        when(tasksByStateDAO.findTasksByState(Collections.singletonList(READY_FOR_POST_PROCESSING))).thenReturn(
                Collections.singletonList(taskByState));

        scheduler.execute();

        verify(postProcessingService).postProcess(taskByState);
    }

}