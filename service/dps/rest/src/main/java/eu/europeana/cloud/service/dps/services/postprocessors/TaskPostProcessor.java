package eu.europeana.cloud.service.dps.services.postprocessors;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 *
 */
public abstract class TaskPostProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskPostProcessor.class);

    protected final TaskStatusChecker taskStatusChecker;
    protected final TaskStatusUpdater taskStatusUpdater;
    protected final HarvestedRecordsDAO harvestedRecordsDAO;

    protected TaskPostProcessor(TaskStatusChecker taskStatusChecker,
                                TaskStatusUpdater taskStatusUpdater,
                                HarvestedRecordsDAO harvestedRecordsDAO) {
        this.taskStatusChecker = taskStatusChecker;
        this.taskStatusUpdater = taskStatusUpdater;
        this.harvestedRecordsDAO = harvestedRecordsDAO;
    }

    /**
     * Executes post processing activity for the provided task
     */
    public void execute(TaskInfo taskInfo, DpsTask dpsTask) {
        if (taskIsDropped(dpsTask)) {
            LOGGER.info("The task {} will not be postprocessed because it was dropped", dpsTask.getTaskId());
            return;
        }
        executePostprocessing(taskInfo, dpsTask);
    }

    protected boolean taskIsDropped(DpsTask dpsTask) {
        return taskStatusChecker.hasDroppedStatus(dpsTask.getTaskId());
    }

    abstract void executePostprocessing(TaskInfo taskInfo, DpsTask dpsTask);

    /**
     * Returns set of names of processed topologies
     */
    abstract Set<String> getProcessedTopologies();


}
