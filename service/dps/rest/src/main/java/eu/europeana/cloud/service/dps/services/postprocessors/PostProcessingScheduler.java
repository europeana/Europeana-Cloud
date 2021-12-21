package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.task.TaskRejectedException;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static eu.europeana.cloud.common.model.dps.TaskState.IN_POST_PROCESSING;
import static eu.europeana.cloud.common.model.dps.TaskState.READY_FOR_POST_PROCESSING;

public class PostProcessingScheduler {

    public static final String SCHEDULE_CRON_RULE = "15,45 * * * * *";
    private static final Logger LOGGER = LoggerFactory.getLogger(PostProcessingScheduler.class);
    private final String applicationId;

    private final TasksByStateDAO tasksByStateDAO;

    private final PostProcessingService postProcessingService;

    private final TaskStatusUpdater taskStatusUpdater;

    public PostProcessingScheduler(
            PostProcessingService postProcessingService,
            TasksByStateDAO tasksByStateDAO,
            TaskStatusUpdater taskStatusUpdater,
            String applicationId) {

        this.postProcessingService = postProcessingService;
        this.tasksByStateDAO = tasksByStateDAO;
        this.taskStatusUpdater = taskStatusUpdater;
        this.applicationId = applicationId;
        LOGGER.info("Post-processing scheduler created with given schedule (cron = '{}')", SCHEDULE_CRON_RULE);
    }

    @Scheduled(cron = SCHEDULE_CRON_RULE)
    public void execute() {
        try {
            findTask(Arrays.asList(IN_POST_PROCESSING, READY_FOR_POST_PROCESSING)).ifPresent(postProcessingService::postProcess);
        } catch (TaskRejectedException e) {
            LOGGER.error("Unable to submit the task for postprocessing", e);
            taskStatusUpdater.setTaskDropped(-1, "Unable to postprocess the task because of: " + e.getMessage());
        }
    }

    private Optional<TaskByTaskState> findTask(List<TaskState> states) {
        LOGGER.info("Looking for tasks in {} state(s)...", states);
        Optional<TaskByTaskState> result = tasksByStateDAO.findTasksByState(states)
                .stream().filter(task -> applicationId.equals(task.getApplicationId())).findFirst();

        result.ifPresentOrElse(
                taskByTaskState -> LOGGER.info("Found task with id = {} to post-process.", taskByTaskState.getId()),
                () -> LOGGER.info("There are no tasks in {} state(s) on this machine.", states)
        );

        return result;
    }
}
