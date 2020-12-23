package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.common.model.dps.RecordState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Component responsible for modifying 'Notifications' table
 */
public class RecordStatusUpdater {

    protected static final int SLEEP_TIME = 5000;
    private static final int DEFAULT_RETRIES = 3;
    private static final Logger LOGGER = LoggerFactory.getLogger(RecordStatusUpdater.class);

    private CassandraSubTaskInfoDAO subTaskInfoDAO;

    public RecordStatusUpdater(CassandraSubTaskInfoDAO subTaskInfoDAO) {
        this.subTaskInfoDAO = subTaskInfoDAO;
    }

    public void addSuccessfullyProcessedRecord(int resourceNum,
                                               long taskId,
                                               String topologyName,
                                               String resource){
        int retries = DEFAULT_RETRIES;

        while (true) {
            try {
                subTaskInfoDAO.insert(
                        resourceNum,
                        taskId,
                        topologyName,
                        resource, RecordState.SUCCESS.name(), null, null, null);
                break;
            } catch (Exception e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while inserting detailed record information to cassandra. Retries left: {}", retries);
                    waitForSpecificTime();
                } else {
                    LOGGER.error("Error while inserting detailed record information to cassandra.");
                    throw e;
                }
            }
        }
    }

    public void addWronglyProcessedRecord(int resourceNum, long taskId, String topologyName, String resource,
                                          String info, String additionalInfo) {

        int retries = DEFAULT_RETRIES;

        while (true) {
            try {
                subTaskInfoDAO.insert(
                        resourceNum,
                        taskId,
                        topologyName,
                        resource, RecordState.ERROR.name(), info, additionalInfo, null);
                break;
            } catch (Exception e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while inserting detailed record information to cassandra. Retries left: {}", retries);
                    waitForSpecificTime();
                } else {
                    LOGGER.error("Error while inserting detailed record information to cassandra.");
                    throw e;
                }
            }
        }
    }

    protected void waitForSpecificTime() {
        try {
            Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            LOGGER.error(e1.getMessage());
        }
    }
}
