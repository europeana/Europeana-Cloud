package eu.europeana.cloud.service.dps.storm;

import eu.europeana.cloud.common.model.dps.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * This is just temporary class that has exactly same behaviour like @{@link NotificationBolt}.
 * Only one difference is that this @{@link OAINotificationBolt} will set result state of OAI task as POST_PROCESSING.
 * Eventually such a task will be postprocessed on DPS application.
 */
public class OAINotificationBolt extends NotificationBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(OAINotificationBolt.class);

    /**
     * Constructor of notification bolt.
     *
     * @param hosts        Cassandra hosts separated by comma (e.g.
     *                     localhost,192.168.47.129)
     * @param port         Cassandra port
     * @param keyspaceName Cassandra keyspace name
     * @param userName     Cassandra username
     * @param password     Cassandra password
     */
    public OAINotificationBolt(String hosts, int port, String keyspaceName, String userName, String password) {
        super(hosts, port, keyspaceName, userName, password);
    }

    @Override
    protected void endTask(NotificationTuple notificationTuple, int errors, int count) {
        taskStatusUpdater.endTask(notificationTuple.getTaskId(), count, errors, "Postprocessing", String.valueOf(TaskState.POST_PROCESSING), new Date());
        LOGGER.info("Task id={} finished topology stage with {} records processed and {} errors. Now it is waiting for postprocessing ",
                notificationTuple.getTaskId(), count, errors);
    }

}
