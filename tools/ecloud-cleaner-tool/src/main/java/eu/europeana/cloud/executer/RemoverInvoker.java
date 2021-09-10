package eu.europeana.cloud.executer;

import eu.europeana.cloud.api.Remover;
import eu.europeana.cloud.api.TaskIdsReader;
import eu.europeana.cloud.readers.CommaSeparatorReaderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created by Tarek on 4/16/2019.
 */
public class RemoverInvoker {
    private final Remover remover;

    static final Logger LOGGER = LoggerFactory.getLogger(RemoverInvoker.class);

    public RemoverInvoker(Remover remover) {
        this.remover = remover;
    }

    public void executeInvokerForSingleTask(long taskId, boolean shouldRemoveErrors) {
        remover.removeNotifications(taskId);
        LOGGER.info("Logs for task Id:{} were removed successfully", taskId);
        LOGGER.info("Removing statistics for:{} was started. This step could take times depending on the size of the task", taskId);
        remover.removeStatistics(taskId);
        LOGGER.info("Statistics for task Id:{} were removed successfully", taskId);
        if (shouldRemoveErrors) {
            remover.removeErrorReports(taskId);
            LOGGER.info("Error reports for task Id:{} were removed successfully", taskId);
        }
    }

    public void executeInvokerForListOfTasks(String filePath, boolean shouldRemoveErrors) throws IOException {
        TaskIdsReader reader = new CommaSeparatorReaderImpl();
        List<String> taskIds = reader.getTaskIds(filePath);
        for (String taskId : taskIds) {
            executeInvokerForSingleTask(Long.valueOf(taskId), shouldRemoveErrors);
        }
    }
}
