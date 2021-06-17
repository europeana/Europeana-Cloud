package eu.europeana.cloud.service.dps.services.task.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.dps.storm.dao.TasksByStateDAO;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static eu.europeana.cloud.common.model.dps.TaskState.IN_POST_PROCESSING;
import static eu.europeana.cloud.common.model.dps.TaskState.READY_FOR_POST_PROCESSING;

public class PostProcessingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostProcessingService.class);
    public static final String SCHEDULE_CRON_RULE = "15,45 * * * * *";

    private CassandraTaskInfoDAO taskInfoDAO;

    private TasksByStateDAO tasksByStateDAO;

    private String applicationIdentifier;

    private HarvestingPostProcessor taskPostProcessor;

    public PostProcessingService(CassandraTaskInfoDAO taskInfoDAO, TasksByStateDAO tasksByStateDAO, String applicationIdentifier, HarvestingPostProcessor taskPostProcessor) {
        this.taskInfoDAO = taskInfoDAO;
        this.tasksByStateDAO = tasksByStateDAO;
        this.applicationIdentifier = applicationIdentifier;
        this.taskPostProcessor = taskPostProcessor;
        LOGGER.info("Created post processing service");
    }

    @Scheduled(cron = SCHEDULE_CRON_RULE)
    public void execute() {
        LOGGER.info("Checking for tasks to post process");
        findTasks(IN_POST_PROCESSING).forEach(this::executeOneTask);
        findTasks(READY_FOR_POST_PROCESSING).forEach(this::executeOneTask);
    }

    public void executeOneTask(long taskId) {
        try {
            taskPostProcessor.execute(loadTask(taskId));
            LOGGER.info("Successfully post processed task id={}", taskId);
        } catch (Exception e) {
            LOGGER.error("Could not post process task id={}", taskId, e);
        }
    }

    private List<Long> findTasks(TaskState state) {
        LOGGER.info("Finding tasks in {} state...", state);
        List<Long> tasks = tasksByStateDAO.findTasksInGivenState(Collections.singletonList(state))
                .stream()
                .filter(task -> applicationIdentifier.equals(task.getOwnerId())).map(TaskInfo::getId)
                .collect(Collectors.toList());

        if (tasks.isEmpty()) {
            LOGGER.info("There are no tasks in {} state on this machine.", state);
        } else {
            LOGGER.info("Found {} tasks to post process ids: {}", tasks.size(), tasks);
        }
        return tasks;
    }

    private DpsTask loadTask(long taskId) throws IOException, TaskInfoDoesNotExistException {
        TaskInfo taskInfo = taskInfoDAO.findById(taskId).orElseThrow(TaskInfoDoesNotExistException::new);
        DpsTask dpsTask = new ObjectMapper().readValue(taskInfo.getTaskDefinition(), DpsTask.class);
        dpsTask.addParameter(PluginParameterKeys.HARVEST_DATE, DateHelper.getISODateString(taskInfo.getSentDate()));
        return dpsTask;
    }

}
