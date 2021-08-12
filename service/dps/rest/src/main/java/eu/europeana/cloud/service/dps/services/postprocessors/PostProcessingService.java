package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static eu.europeana.cloud.common.model.dps.TaskState.IN_POST_PROCESSING;
import static eu.europeana.cloud.common.model.dps.TaskState.READY_FOR_POST_PROCESSING;

public class PostProcessingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostProcessingService.class);

    public static final String SCHEDULE_CRON_RULE = "15,45 * * * * *";

    private static final String MESSAGE_SUCCESSFULLY_POST_PROCESSED = "Successfully post processed task with id={}";
    private static final String MESSAGE_FAILED_POST_PROCESSED = "Could not post process task with id={}";

    private final String applicationId;

    private final CassandraTaskInfoDAO taskInfoDAO;

    private final TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;

    private final TasksByStateDAO tasksByStateDAO;

    private final TaskStatusUpdater taskStatusUpdater;

    private final PostProcessorFactory postProcessorFactory;

    public PostProcessingService(PostProcessorFactory postProcessorFactory,
                                 CassandraTaskInfoDAO taskInfoDAO,
                                 TaskDiagnosticInfoDAO taskDiagnosticInfoDAO,
                                 TasksByStateDAO tasksByStateDAO,
                                 TaskStatusUpdater taskStatusUpdater,
                                 String applicationId) {

        this.postProcessorFactory = postProcessorFactory;
        this.taskInfoDAO = taskInfoDAO;
        this.taskDiagnosticInfoDAO = taskDiagnosticInfoDAO;
        this.tasksByStateDAO = tasksByStateDAO;
        this.taskStatusUpdater = taskStatusUpdater;
        this.applicationId = applicationId;
        LOGGER.info("Post-processing service created with given schedule (cron = '{}')", SCHEDULE_CRON_RULE);
    }

    @Scheduled(cron = SCHEDULE_CRON_RULE)
    public void execute() {
        findTask(Arrays.asList(IN_POST_PROCESSING, READY_FOR_POST_PROCESSING)).ifPresent(this::postProcess);
    }

    public void postProcess(TaskByTaskState taskByTaskState) {
        try {
            TaskPostProcessor postProcessor = postProcessorFactory.getPostProcessor(taskByTaskState);
            var taskInfo = loadTask(taskByTaskState);
            DpsTask task = DpsTask.fromTaskInfo(taskInfo);
            taskDiagnosticInfoDAO.updatePostprocessingStartTime(taskByTaskState.getId(), Instant.now());
            postProcessor.execute(taskInfo, task);
            LOGGER.info(MESSAGE_SUCCESSFULLY_POST_PROCESSED, taskByTaskState.getId());
        } catch (IOException | TaskInfoDoesNotExistException | PostProcessingException exception) {
            LOGGER.error(MESSAGE_FAILED_POST_PROCESSED, taskByTaskState.getId(), exception);
            taskStatusUpdater.setTaskDropped(taskByTaskState.getId(), exception.getMessage());
        }
    }

    private Optional<TaskByTaskState> findTask(List<TaskState> states) {
        LOGGER.info("Looking for tasks in {} state(s)...", states);
        Optional<TaskByTaskState>  result = tasksByStateDAO.findTasksByState(states)
                .stream().filter(task -> applicationId.equals(task.getApplicationId())).findFirst();

        result.ifPresentOrElse(
                taskByTaskState -> LOGGER.info("Found task with id = {} to post-process.", taskByTaskState.getId()),
                () -> LOGGER.info("There are no tasks in {} state(s) on this machine.", states)
        );

        return result;
    }

    private TaskInfo loadTask(TaskByTaskState taskByTaskState) throws TaskInfoDoesNotExistException {
        return taskInfoDAO.findById(taskByTaskState.getId()).orElseThrow(TaskInfoDoesNotExistException::new);
    }
}
