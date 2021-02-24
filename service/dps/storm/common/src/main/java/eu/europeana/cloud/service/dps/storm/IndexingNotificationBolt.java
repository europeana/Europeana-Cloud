package eu.europeana.cloud.service.dps.storm;

import eu.europeana.cloud.client.dps.rest.DpsClient;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Created by Tarek on 9/24/2019.
 */
public class IndexingNotificationBolt extends NotificationBolt {
    private static final String AUTHORIZATION = "Authorization";
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexingNotificationBolt.class);
    private final String dpsServer;
    private final String indexingTopologyName;

    public IndexingNotificationBolt(String hosts, int port, String keyspaceName,
                                    String userName, String password, String dpsServer, String indexingTopologyName) {
        super(hosts, port, keyspaceName, userName, password);
        this.dpsServer = dpsServer;
        this.indexingTopologyName = indexingTopologyName;
    }

    @Override
    protected void endTask(NotificationTuple notificationTuple, int errors, int count) {
        long taskId = notificationTuple.getTaskId();
        DpsClient dpsClient = null;
        try {
            taskStatusUpdater.endTask(taskId, count, errors, TaskState.REMOVING_FROM_SOLR_AND_MONGO.toString(), TaskState.REMOVING_FROM_SOLR_AND_MONGO.toString(), new Date());
            dpsClient = new DpsClient(dpsServer);
            DataSetCleanerParameters dataSetCleanerParameters = (DataSetCleanerParameters) notificationTuple.getParameter(NotificationParameterKeys.DATA_SET_CLEANING_PARAMETERS);
            LOGGER.info("DataSet {} will be sent to be cleaned", dataSetCleanerParameters.getDataSetId());
            dpsClient.cleanMetisIndexingDataset(indexingTopologyName, taskId, dataSetCleanerParameters,
                    AUTHORIZATION, notificationTuple.getParameter(NotificationParameterKeys.AUTHORIZATION_HEADER).toString());
            LOGGER.info("DataSet {} is sent to be cleaned and the task is finished successfully from within Storm", dataSetCleanerParameters.getDataSetId());
        } catch (Exception e) {
            LOGGER.error("An error happened while ending the task ", e);
            taskStatusUpdater.setTaskDropped(taskId, e.getMessage());
        } finally {
            if (dpsClient != null)
                dpsClient.close();
        }
    }
}
