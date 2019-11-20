package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTasksDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

/**
 * This component will check all tasks with status TaskState.PROCESSING_BY_REST_APPLICATION ({@link eu.europeana.cloud.common.model.dps.TaskState})
 * and start harvesting again for them.
 */
@Component
public class UnfinishedTasksExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnfinishedTasksExecutor.class);

    @Autowired
    private CassandraTasksDAO tasksDAO;

    @Autowired
    private String applicationIdentifier;

    @PostConstruct
    public void reRunUnfinishedTasks() {
        LOGGER.info("Will restart all pending tasks");
        List<TaskInfo> results = findProcessingByRestTasks();
        List<TaskInfo> resultsForCurrentMachine = findTasksForCurrentMachine(results);
        restartExecutionFor(resultsForCurrentMachine);
    }

    private List<TaskInfo> findProcessingByRestTasks() {
        LOGGER.info("Searching for all unfinished tasks");
        return tasksDAO.findTasksInGivenState(TaskState.PROCESSING_BY_REST_APPLICATION);
    }

    private List<TaskInfo> findTasksForCurrentMachine(List<TaskInfo> results) {
        LOGGER.info("Filtering tasks for current machine: {}" + applicationIdentifier);
        List<TaskInfo> result = new ArrayList<>();
        for (TaskInfo taskInfo : results) {
            if (taskInfo.getOwnerId().equals(applicationIdentifier)) {
                result.add(taskInfo);
            }
        }
        return result;
    }

    private void restartExecutionFor(List<TaskInfo> tasksToBeRestarted) {
        if (tasksToBeRestarted.size() == 0) {
            LOGGER.info("No tasks to be restarted");
        } else {
            for (TaskInfo taskInfo : tasksToBeRestarted) {
                LOGGER.info("Restarting execution for: {}" + tasksToBeRestarted);
                //we should use here another Spring component
            }
        }
    }
}
