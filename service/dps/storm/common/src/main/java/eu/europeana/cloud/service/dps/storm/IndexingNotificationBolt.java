package eu.europeana.cloud.service.dps.storm;

import eu.europeana.cloud.client.dps.rest.DpsClient;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.metis.indexing.DataSetCleanerParameters;

import java.util.Date;

/**
 * Created by Tarek on 9/24/2019.
 */
public class IndexingNotificationBolt extends NotificationBolt {

    public IndexingNotificationBolt(String hosts, int port, String keyspaceName,
                                    String userName, String password) {
        super(hosts, port, keyspaceName, userName, password);
    }

    @Override
    protected void endTask(NotificationTuple notificationTuple, int errors, int count) {
        long taskId = notificationTuple.getTaskId();
        DpsClient dpsClient = null;
        try {
            taskInfoDAO.endTask(taskId, count, errors, TaskState.REMOVING_FROM_SOLR_AND_MONGO.toString(), TaskState.REMOVING_FROM_SOLR_AND_MONGO.toString(), new Date());
            dpsClient = new DpsClient(notificationTuple.getParameter(NotificationParameterKeys.DPS_URL).toString());
            DataSetCleanerParameters dataSetCleanerParameters = (DataSetCleanerParameters) notificationTuple.getParameter(NotificationParameterKeys.DATA_SET_CLEANING_PARAMETERS);
            dpsClient.cleanMetisIndexingDataset("indexing_topology", taskId, dataSetCleanerParameters);
        } catch (Exception e) {
            taskInfoDAO.dropTask(taskId, e.getMessage(), TaskState.DROPPED.toString());
        } finally {
            if (dpsClient != null)
                dpsClient.close();
        }
    }
}
