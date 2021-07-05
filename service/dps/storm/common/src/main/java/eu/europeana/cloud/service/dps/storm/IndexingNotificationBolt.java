package eu.europeana.cloud.service.dps.storm;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;

import java.io.IOException;

/**
 * Created by Tarek on 9/24/2019.
 */
public class IndexingNotificationBolt extends NotificationBolt {
    public IndexingNotificationBolt(String hosts, int port, String keyspaceName,
                                    String userName, String password) {
        super(hosts, port, keyspaceName, userName, password);
    }

    @Override
    protected boolean needsPostProcessing(NotificationTuple tuple) throws TaskInfoDoesNotExistException, IOException {
        return !isIncrementalIndexing(tuple);
    }

    private boolean isIncrementalIndexing(NotificationTuple tuple) throws IOException, TaskInfoDoesNotExistException {
        return "true".equals(loadDpsTask(tuple).getParameter(PluginParameterKeys.INCREMENTAL_INDEXING));
    }

}
