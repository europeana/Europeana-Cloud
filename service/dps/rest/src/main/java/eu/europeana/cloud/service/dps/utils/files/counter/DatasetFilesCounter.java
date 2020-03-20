package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Tarek on 4/6/2016.
 * File counters inside a dataset task
 */
public class DatasetFilesCounter extends FilesCounter {
    public final static int UNKNOWN_EXPECTED_SIZE = -1;
    private CassandraTaskInfoDAO taskDAO;
    private final static Logger LOGGER = LoggerFactory.getLogger(DatasetFilesCounter.class);

    DatasetFilesCounter(CassandraTaskInfoDAO taskDAO) {
        this.taskDAO = taskDAO;
    }

    public int getFilesCount(DpsTask task) throws TaskSubmissionException {
        String providedTaskId = task.getParameter(PluginParameterKeys.PREVIOUS_TASK_ID);
        if (providedTaskId==null)
            return UNKNOWN_EXPECTED_SIZE;
        try {
            long taskId = Long.parseLong(providedTaskId);
            TaskInfo taskInfo = taskDAO.searchById(taskId);
            return taskInfo.getProcessedElementCount();
        } catch (NumberFormatException e) {
            LOGGER.error("The provided previous task id {} is not long  ", providedTaskId);
            return UNKNOWN_EXPECTED_SIZE;
        } catch (TaskInfoDoesNotExistException e) {
            LOGGER.error("Task with taskId {} doesn't exist ", providedTaskId);
            return UNKNOWN_EXPECTED_SIZE;
        } catch (Exception e) {
            LOGGER.error("he task was dropped because of {} ", e.getMessage());
            throw new TaskSubmissionException("The task was dropped while counting the files number because of " + e.getMessage());
        }
    }
}
