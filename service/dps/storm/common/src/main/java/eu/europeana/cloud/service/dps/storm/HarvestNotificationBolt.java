package eu.europeana.cloud.service.dps.storm;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This is just temporary class that has exactly same behaviour like @{@link NotificationBolt}.
 * Only one difference is that this @{@link HarvestNotificationBolt} will set result state of OAI task as POST_PROCESSING.
 * Eventually such a task will be post processed on DPS application.
 */
public class HarvestNotificationBolt extends NotificationBolt {

    private static final Logger LOGGER = LoggerFactory.getLogger(HarvestNotificationBolt.class);

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
    public HarvestNotificationBolt(String hosts, int port, String keyspaceName, String userName, String password) {
        super(hosts, port, keyspaceName, userName, password);
    }

    @Override
    protected boolean needsPostProcessing(NotificationTuple tuple) throws TaskInfoDoesNotExistException, IOException {
        return isIncrementalHarvesting(tuple);
    }

    private boolean isIncrementalHarvesting(NotificationTuple tuple) throws IOException, TaskInfoDoesNotExistException {
        return "true".equals(loadDpsTask(tuple).getParameter(PluginParameterKeys.INCREMENTAL_HARVEST));
    }

}
