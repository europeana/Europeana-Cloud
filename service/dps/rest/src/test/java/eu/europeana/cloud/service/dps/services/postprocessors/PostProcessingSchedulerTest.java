package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import static eu.europeana.cloud.common.model.dps.TaskState.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class PostProcessingSchedulerTest {


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

    @InjectMocks
    private PostProcessingScheduler scheduler;

    @Before
    public void setup() {
        scheduler = new PostProcessingScheduler(postProcessingService, tasksByStateDAO, taskInfoDAO, taskStatusUpdater, APPLICATION_ID);
    }

    //Activities on start - init() method
    @Test
    public void shouldResetStateOfInPostProcessingTasksOnInit() {
        TaskByTaskState task = TaskByTaskState.builder().id(TASK_ID).state(IN_POST_PROCESSING)
                .applicationId(APPLICATION_ID).build();
        when(tasksByStateDAO.findTasksByState(Collections.singletonList(IN_POST_PROCESSING)))
                .thenReturn(Collections.singletonList(task));

        scheduler.init();

        verify(taskStatusUpdater).updateState(eq(TASK_ID), eq(TaskState.READY_FOR_POST_PROCESSING), anyString());
    }

    //Scheduled activities - execute method()
    @Test
    public void shouldMarkQueuedTaskAsCompletedWhenAllRecordsProcessedAndTaskDoesNotNeedPostprocessing() {
        TaskByTaskState taskByState = TaskByTaskState.builder().id(TASK_ID).state(QUEUED).applicationId(APPLICATION_ID).build();
        when(tasksByStateDAO.findTasksByState(Collections.singletonList(QUEUED))).thenReturn(Collections.singletonList(taskByState));
        TaskInfo taskInfo=TaskInfo.builder().id(TASK_ID).state(QUEUED).expectedRecordsNumber(60).
                processedRecordsCount(30).ignoredRecordsCount(20).deletedRecordsCount(10).build();
        when(taskInfoDAO.findById(TASK_ID)).thenReturn(Optional.of(taskInfo));

        scheduler.execute();

        verify(taskStatusUpdater).setTaskCompletelyProcessed(eq(TASK_ID), anyString());
    }

    @Test
    public void shouldMarkQueuedTaskAsReadyForPostProcessingWhenAllRecordsProcessedAndTaskDoesNotNeedPostprocessing() throws IOException {
        TaskByTaskState taskByState = TaskByTaskState.builder().id(TASK_ID).state(QUEUED).applicationId(APPLICATION_ID).build();
        when(tasksByStateDAO.findTasksByState(Collections.singletonList(QUEUED))).thenReturn(Collections.singletonList(taskByState));
        TaskInfo taskInfo=TaskInfo.builder().id(TASK_ID).state(QUEUED).expectedRecordsNumber(60).
                processedRecordsCount(30).ignoredRecordsCount(20).deletedRecordsCount(10).build();
        when(taskInfoDAO.findById(TASK_ID)).thenReturn(Optional.of(taskInfo));
        when(postProcessingService.needsPostprocessing(any(),any())).thenReturn(true);

        scheduler.execute();

        verify(taskStatusUpdater).updateState(eq(TASK_ID), eq(READY_FOR_POST_PROCESSING), anyString());

    }

    @Test
    public void shouldIgnoreQueuedTaskIfNotAllRecordsProcessed() {
        TaskByTaskState taskByState = TaskByTaskState.builder().id(TASK_ID).state(QUEUED).applicationId(APPLICATION_ID).build();
        when(tasksByStateDAO.findTasksByState(Collections.singletonList(QUEUED))).thenReturn(Collections.singletonList(taskByState));
        TaskInfo taskInfo=TaskInfo.builder().id(TASK_ID).state(QUEUED).expectedRecordsNumber(61).
                processedRecordsCount(30).ignoredRecordsCount(20).deletedRecordsCount(10).build();
        when(taskInfoDAO.findById(TASK_ID)).thenReturn(Optional.of(taskInfo));

        scheduler.execute();

        verifyNoInteractions(taskStatusUpdater);
        verifyNoInteractions(postProcessingService);
    }

    @Test
    public void shouldSendReadyTasksToPostProcessing() {
        TaskByTaskState taskByState = TaskByTaskState.builder().id(TASK_ID).state(READY_FOR_POST_PROCESSING).applicationId(APPLICATION_ID).build();
        when(tasksByStateDAO.findTasksByState(Collections.singletonList(READY_FOR_POST_PROCESSING))).thenReturn(Collections.singletonList(taskByState));

        scheduler.execute();

        verify(postProcessingService).postProcess(taskByState);
    }

}