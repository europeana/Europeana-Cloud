package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskByTaskState;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import javax.inject.Inject;
import java.io.IOException;
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

    private CassandraTaskInfoDAO taskInfoDAO;

    private TasksByStateDAO tasksByStateDAO;

    private PostProcessorFactory postProcessorFactory;

    @Inject
    public PostProcessingService(PostProcessorFactory postProcessorFactory,
                                 CassandraTaskInfoDAO taskInfoDAO,
                                 TasksByStateDAO tasksByStateDAO) {

        this.postProcessorFactory = postProcessorFactory;
        this.taskInfoDAO = taskInfoDAO;
        this.tasksByStateDAO = tasksByStateDAO;
        LOGGER.info("Created post processing service");
    }

    @Scheduled(cron = SCHEDULE_CRON_RULE)
    public void execute() {
        findTask(Arrays.asList(IN_POST_PROCESSING, READY_FOR_POST_PROCESSING)).ifPresent(this::postProcess);
    }

    public void postProcess(TaskByTaskState taskByTaskState) {
        postProcessorFactory.getPostProcessor(taskByTaskState).ifPresent(taskPostProcessor -> {
            try {
                taskPostProcessor.execute(loadTask(taskByTaskState.getId()));
                LOGGER.info(MESSAGE_SUCCESSFULLY_POST_PROCESSED, taskByTaskState.getId());
            } catch (IOException | TaskInfoDoesNotExistException exception) {
                LOGGER.error(MESSAGE_FAILED_POST_PROCESSED, taskByTaskState.getId(), exception);
            }
        });
    }

    private Optional<TaskByTaskState> findTask(List<TaskState> state) {
        LOGGER.info("Finding tasks in {} state...", state);
        Optional<TaskByTaskState> result = tasksByStateDAO.findTaskByState(state);

        if (result.isPresent()) {
            LOGGER.info("Found task to post process with id= {}", result.get());
        } else {
            LOGGER.info("There are no tasks in {} state on this machine.", state);
        }

        return result;
    }

    public DpsTask loadTask(long taskId) throws IOException, TaskInfoDoesNotExistException {
        var taskInfo = taskInfoDAO.findById(taskId).orElseThrow(TaskInfoDoesNotExistException::new);
        return DpsTask.fromTaskInfo(taskInfo);
    }
}
