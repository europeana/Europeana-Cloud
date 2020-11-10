package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.services.submitters.TaskSubmitter;
import eu.europeana.cloud.service.dps.services.submitters.TaskSubmitterFactory;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TasksByStateDAO;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * This component will check all tasks with status TaskState.PROCESSING_BY_REST_APPLICATION
 * ({@link eu.europeana.cloud.common.model.dps.TaskState})
 * and start harvesting again for them.
 */
@Service
public class UnfinishedTasksExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnfinishedTasksExecutor.class);
    public static final List<TaskState> RESUMABLE_TASK_STATES = Arrays.asList(TaskState.PROCESSING_BY_REST_APPLICATION, TaskState.DEPUBLISHING);

    private final TasksByStateDAO tasksDAO;
    private final CassandraTaskInfoDAO taskInfoDAO;
    private final TaskSubmitterFactory taskSubmitterFactory;
    private final String applicationIdentifier;
    private final TaskStatusUpdater taskStatusUpdater;

    UnfinishedTasksExecutor(TasksByStateDAO tasksDAO,
                            CassandraTaskInfoDAO taskInfoDAO,
                            TaskSubmitterFactory taskSubmitterFactory,
                            String applicationIdentifier,
                            TaskStatusUpdater taskStatusUpdater) {
        this.tasksDAO = tasksDAO;
        this.taskSubmitterFactory = taskSubmitterFactory;
        this.taskInfoDAO = taskInfoDAO;
        this.applicationIdentifier = applicationIdentifier;
        this.taskStatusUpdater = taskStatusUpdater;
    }

    @PostConstruct
    public void reRunUnfinishedTasks() {
        LOGGER.info("Will restart all pending tasks");
        List<TaskInfo> results = findProcessingByRestTasks();
        List<TaskInfo> tasksForCurrentMachine = findTasksForCurrentMachine(results);
        resumeExecutionFor(tasksForCurrentMachine);
    }

    private List<TaskInfo> findProcessingByRestTasks() {
        LOGGER.info("Searching for all unfinished tasks");
        return tasksDAO.findTasksInGivenState(RESUMABLE_TASK_STATES);
    }

    private List<TaskInfo> findTasksForCurrentMachine(List<TaskInfo> results) {
        LOGGER.info("Filtering tasks for current machine: {}", applicationIdentifier);
        List<TaskInfo> result = new ArrayList<>();
        for (TaskInfo taskInfo : results) {
            if (taskInfo.getOwnerId().equals(applicationIdentifier)) {
                taskInfoDAO.findById(taskInfo.getId()).ifPresentOrElse(result::add, () ->
                        LOGGER.warn("Task with id {} not found in basic_info table. It will be ignored in resumption process.", taskInfo.getId()));

            }
        }
        return result;
    }

    private void resumeExecutionFor(List<TaskInfo> tasksToBeRestarted) {
        if (tasksToBeRestarted.isEmpty()) {
            LOGGER.info("No tasks to be resumed");
        } else {
            for (TaskInfo taskInfo : tasksToBeRestarted) {
                resumeTask(taskInfo);
            }
        }
    }

    private void resumeTask(TaskInfo taskInfo) {
        try {
            LOGGER.info("Resuming execution for: {}", taskInfo);
            SubmitTaskParameters submitTaskParameters = prepareSubmitTaskParameters(taskInfo);
            TaskSubmitter taskSubmitter = taskSubmitterFactory.provideTaskSubmitter(submitTaskParameters);
            taskSubmitter.submitTask(submitTaskParameters);
        } catch (IOException | TaskSubmissionException e) {
            LOGGER.error("Unable to resume the task", e);
            taskStatusUpdater.setTaskDropped(taskInfo.getId(), ExceptionUtils.getStackTrace(e));
        }
    }

    private SubmitTaskParameters prepareSubmitTaskParameters(TaskInfo taskInfo) throws IOException {
        DpsTask dpsTask = new ObjectMapper().readValue(taskInfo.getTaskDefinition(), DpsTask.class);
        return SubmitTaskParameters.builder()
                .sentTime(new Date())
                .task(dpsTask)
                .topologyName(taskInfo.getTopologyName())
                .status(TaskState.PROCESSING_BY_REST_APPLICATION)
                .info("The task is in a pending mode, it is being processed before submission")
                .taskJSON(taskInfo.getTaskDefinition())
                .restarted(true).build();
    }
}
