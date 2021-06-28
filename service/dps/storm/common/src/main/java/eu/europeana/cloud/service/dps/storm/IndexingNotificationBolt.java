package eu.europeana.cloud.service.dps.storm;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Tarek on 9/24/2019.
 */
public class IndexingNotificationBolt extends NotificationBolt {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingNotificationBolt.class);

    public IndexingNotificationBolt(String hosts, int port, String keyspaceName,
                                    String userName, String password) {
        super(hosts, port, keyspaceName, userName, password);
    }

    @Override
    protected void endTask(NotificationTuple notificationTuple, int errors, int count) {
        if (isIncrementalIndexing(notificationTuple)) {
            setTaskStatusTo(TaskState.READY_FOR_POST_PROCESSING, notificationTuple, errors, count);
        } else {
            setTaskStatusTo(TaskState.PROCESSED, notificationTuple, errors, count);
        }
    }

    private boolean isIncrementalIndexing(NotificationTuple tuple) {
        return "true".equals(tuple.getParameter(PluginParameterKeys.INCREMENTAL_INDEXING));
    }

    private void setTaskStatusTo(TaskState taskState, NotificationTuple notificationTuple, int errors, int count) {
        taskStatusUpdater.updateState(notificationTuple.getTaskId(), taskState,
                "Ready for post processing after topology stage is finished");
        LOGGER.info("Task id={} finished topology stage with {} records processed and {} errors. Now it is waiting for post processing ",
                notificationTuple.getTaskId(), count, errors);
    }
}
