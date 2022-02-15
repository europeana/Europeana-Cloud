package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.storm.dao.NotificationsDAO;

import java.util.Map;

/**
 * Component responsible for modifying 'Notifications' table
 */
public class RecordStatusUpdater {

    private NotificationsDAO subTaskInfoDAO;

    public RecordStatusUpdater(NotificationsDAO subTaskInfoDAO) {
        this.subTaskInfoDAO = subTaskInfoDAO;
    }

    public void addSuccessfullyProcessedRecord(int resourceNum,
                                               long taskId,
                                               String topologyName,
                                               String resource) {
        subTaskInfoDAO.insert(resourceNum, taskId, topologyName, resource, RecordState.SUCCESS.name(), null,
                null, null);
    }

    public void addWronglyProcessedRecord(int resourceNum, long taskId, String topologyName,
                                          String resource, String info, String additionalInfo) {
        subTaskInfoDAO.insert(resourceNum, taskId, topologyName, resource,
                RecordState.ERROR.name(), info, Map.of(NotificationsDAO.AUXILIARY_KEY, additionalInfo), null);
    }

}
