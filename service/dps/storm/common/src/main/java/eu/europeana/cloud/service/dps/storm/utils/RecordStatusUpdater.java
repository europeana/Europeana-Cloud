package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;

/**
 * Component responsible for modifying 'Notifications' table
 */
public class RecordStatusUpdater {

    private CassandraSubTaskInfoDAO subTaskInfoDAO;

    public RecordStatusUpdater(CassandraSubTaskInfoDAO subTaskInfoDAO) {
        this.subTaskInfoDAO = subTaskInfoDAO;
    }

    public void addSuccessfullyProcessedRecord(int resourceNum,
                                               long taskId,
                                               String topologyName,
                                               String resource) {
        RetryableMethodExecutor.executeOnDb("Error while inserting detailed record information to cassandra", () ->
                subTaskInfoDAO.insert(
                        resourceNum,
                        taskId,
                        topologyName,
                        resource, RecordState.SUCCESS.name(), null, null, null));

    }

    public void addWronglyProcessedRecord(int resourceNum, long taskId, String topologyName, String resource,
                                          String info, String additionalInfo) {
        RetryableMethodExecutor.executeOnDb("Error while inserting detailed record information to cassandra", () ->
                subTaskInfoDAO.insert(
                        resourceNum,
                        taskId,
                        topologyName,
                        resource, RecordState.ERROR.name(), info, additionalInfo, null));
    }

}
