package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.annotation.Scheduled;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static eu.europeana.cloud.common.model.dps.TaskState.*;

/**
 * Component responsible for executing postprocessing for the tasks.
 * It uses Scheduler for that.
 */
public class PostProcessingScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostProcessingScheduler.class);
    private final String applicationId;

    private final TasksByStateDAO tasksByStateDAO;

    private final PostProcessingService postProcessingService;

    private final TaskStatusUpdater taskStatusUpdater;
    private final CassandraTaskInfoDAO taskInfoDAO;

    public PostProcessingScheduler(
            PostProcessingService postProcessingService,
            TasksByStateDAO tasksByStateDAO,
            CassandraTaskInfoDAO taskInfoDAO,
            TaskStatusUpdater taskStatusUpdater,
            String applicationId) {

        this.postProcessingService = postProcessingService;
        this.tasksByStateDAO = tasksByStateDAO;
        this.taskInfoDAO = taskInfoDAO;
        this.taskStatusUpdater = taskStatusUpdater;
        this.applicationId = applicationId;
        LOGGER.info("Post-processing scheduler created.");
    }

    @PostConstruct
    public void init() {
        LOGGER.debug("Initializing Postprocessing scheduler");
        resetTasksState();
    }


    @Scheduled(fixedRate = 15_000, initialDelay = 60_000)
    public void execute() {
        findTasksIn(List.of(QUEUED)).forEach(this::checkFinishedStormWork);
        findTasksIn(List.of(READY_FOR_POST_PROCESSING)).forEach(this::executePostProcessingAsync);
    }

    private void checkFinishedStormWork(TaskByTaskState taskByTaskState) {
        TaskInfo task = taskInfoDAO.findById(taskByTaskState.getId()).orElseThrow();
        if (taskInQueueStateAlsoInTaskInfoTable(task) && areAllRecordsCompletedOnStorm(task)) {
            handleAllRecordsOnStormCompleted(taskByTaskState, task);
        }
    }

    private void handleAllRecordsOnStormCompleted(TaskByTaskState taskState, TaskInfo taskInfo) {
        boolean needsPostprocessing;
        try {
            needsPostprocessing = postProcessingService.needsPostprocessing(taskState, taskInfo);
        } catch (IOException e) {
            //It could happen when task configuration already in memory could not be deserialized, so it is
            //rather permanent problem and task should be dropped
            LOGGER.error("Unable to check if task should be post-processed id: {}!", taskInfo.getId(), e);
            taskStatusUpdater.setTaskDropped(taskState.getId(),
                    "Unable to check if task should be post-processed because of: " + e.getMessage());
            return;
        }

        if (needsPostprocessing) {
            markTaskReadyForPostprocessing(taskInfo);
        } else {
            markTaskCompleted(taskInfo);
        }
    }

    private void markTaskCompleted(TaskInfo task) {
        taskStatusUpdater.setTaskCompletelyProcessed(task.getId(), PROCESSED.getDefaultMessage());
        LOGGER.info("Task completed, id: {}.", task.getId());
    }

    private void markTaskReadyForPostprocessing(TaskInfo task) {
        taskStatusUpdater.updateState(task.getId(), READY_FOR_POST_PROCESSING, READY_FOR_POST_PROCESSING.getDefaultMessage());
        LOGGER.info("Task was marked for postprocessing, id: {}.", task.getId());
    }

    private void executePostProcessingAsync(TaskByTaskState taskByTaskState) {
        try {
            postProcessingService.postProcess(taskByTaskState);
            LOGGER.info("Task was sent to post-processing, id: {}.", taskByTaskState.getId());
        } catch (TaskRejectedException e) {
            LOGGER.error("Unable to submit the task id: {} for postprocessing", taskByTaskState.getId(), e);
            taskStatusUpdater.setTaskDropped(taskByTaskState.getId(),
                    "Unable to postprocess the task because of: " + e.getMessage());
        }
    }

    public void resetTasksState() {
        LOGGER.info("Resetting state of all pending tasks to {}", IN_POST_PROCESSING);
        findTasksIn(List.of(IN_POST_PROCESSING)).forEach(this::resetTaskState);
    }

    private List<TaskByTaskState> findTasksIn(List<TaskState> states) {
        LOGGER.debug("Looking for tasks in {} state(s)...", states);
        List<TaskByTaskState> tasks = tasksByStateDAO.findTasksByState(states)
                .stream().filter(task -> applicationId.equals(task.getApplicationId())).collect(Collectors.toList());
        LOGGER.debug("Found tasks in {} state(s) : {}", states, tasks);
        return tasks;
    }

    private void resetTaskState(TaskByTaskState taskByTaskState) {
        LOGGER.info("Resetting state of task {} to {}", taskByTaskState.getId(), IN_POST_PROCESSING);
        taskStatusUpdater.updateState(taskByTaskState.getId(), READY_FOR_POST_PROCESSING, "Resetting task state");
    }

    private boolean areAllRecordsCompletedOnStorm(TaskInfo task) {
        return (task.getProcessedRecordsCount() + task.getIgnoredRecordsCount() + task.getDeletedRecordsCount())
                == task.getExpectedRecordsNumber();
    }

    private boolean taskInQueueStateAlsoInTaskInfoTable(TaskInfo task) {
        return TaskState.QUEUED == task.getState();
    }

}
