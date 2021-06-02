package eu.europeana.cloud.service.dps.services.task.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.DateHelper;
import eu.europeana.cloud.service.dps.storm.utils.TasksByStateDAO;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class PostProcessingService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostProcessingService.class);

    @Autowired
    private CassandraTaskInfoDAO taskInfoDAO;

    @Autowired
    private TasksByStateDAO tasksByStateDAO;

    @Autowired
    private String applicationIdentifier;

    @Autowired
    private DeletedRecordsForIncrementalHarvestingPostProcessor taskPostprocessor;


    @Scheduled(cron = "15,45 * * * * *")
    public void go() {
        findTasksForPostprocess().forEach(this::executePostprocess);
    }

    private List<Long> findTasksForPostprocess() {
        LOGGER.info("Checking for tasks to postprocess...");
        List<Long> tasks = tasksByStateDAO.findTasksInGivenState(Collections.singletonList(TaskState.POST_PROCESSING))
                .stream()
                .filter(task -> applicationIdentifier.equals(task.getOwnerId())).map(TaskInfo::getId)
                .collect(Collectors.toList());

        if (tasks.isEmpty()) {
            LOGGER.info("There are no tasks to postprocess on this machine.");
        } else {
            LOGGER.info("Found {} tasks to postprocess ids: {}", tasks.size(), tasks);
        }
        return tasks;
    }

    public void executePostprocess(long taskId) {
        try {
            taskPostprocessor.execute(loadTask(taskId));
            LOGGER.info("Successfully postprocessed task id={}", taskId);
        } catch (Exception e) {
            LOGGER.error("Could not postprocess task id={}", taskId, e);
        }
    }

    private DpsTask loadTask(long taskId) throws IOException, TaskInfoDoesNotExistException {
        TaskInfo taskInfo = taskInfoDAO.findById(taskId).orElseThrow(TaskInfoDoesNotExistException::new);
        DpsTask dpsTask = new ObjectMapper().readValue(taskInfo.getTaskDefinition(), DpsTask.class);
        dpsTask.addParameter(PluginParameterKeys.HARVEST_DATE, DateHelper.getISODateString(taskInfo.getSentDate()));
        return dpsTask;
    }

}
