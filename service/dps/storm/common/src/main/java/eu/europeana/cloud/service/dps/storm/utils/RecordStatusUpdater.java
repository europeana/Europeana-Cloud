package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.dps.RecordState;

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
                subTaskInfoDAO.insert(
                        resourceNum,
                        taskId,
                        topologyName,
                resource, RecordState.SUCCESS.name(), null, null, null);
    }

    public void addWronglyProcessedRecord(int resourceNum, long taskId, String topologyName, String resource,
                                          String info, String additionalInfo) {
                subTaskInfoDAO.insert(
                        resourceNum,
                        taskId,
                        topologyName,
                        resource, RecordState.ERROR.name(), info, additionalInfo, null);
    }

}
