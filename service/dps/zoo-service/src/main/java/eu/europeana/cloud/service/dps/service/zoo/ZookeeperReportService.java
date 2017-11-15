package eu.europeana.cloud.service.dps.service.zoo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.service.coordination.ZookeeperService;
import eu.europeana.cloud.service.coordination.configuration.DynamicPropertyManager;
import eu.europeana.cloud.service.coordination.configuration.ZookeeperDynamicPropertyManager;
import eu.europeana.cloud.service.dps.TaskExecutionReportService;
import eu.europeana.cloud.service.dps.exception.AccessDeniedOrObjectDoesNotExistException;

/**
 * Stores / retrieves dps tasks and task progress/reports from / to Kafka
 * topics.
 * <p/>
 * Progress notifications are stored in Zookeeper directories as direct ZK child nodes of the root parent ZK node
 * (which is specified in {@link #ZOOKEEPER_PATH}).
 * <p/>
 * Example:
 * <p/>
 * An example property node is "/eCloud/v2/ISTI/configuration/tasks/xslt-32427389328/progress".
 * For the above example, an example-value for "progress", for task "xslt-32427389328" would be "50",
 * that indicates that current progress is at 50%.
 *
 * @author manos
 */
public class ZookeeperReportService implements TaskExecutionReportService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperReportService.class);

    private final static int ZOOKEEPER_CONNECTION_TIME = 3000;
    private final static int ZOOKEEPER_SESSION_TIMEOUT = 3000;

    /**
     * Key used to store TaskExecution progress in a Zookeeper node
     */
    private final static String PROGRESS_KEY = "progress";

    /**
     * Base path where all metrics are stored.
     */
    private final static String ZOOKEEPER_PATH = "/dps-tasks";

    private ZookeeperService zS;
    private DynamicPropertyManager pManager;

    public ZookeeperReportService(ZookeeperService zS, DynamicPropertyManager pManager) {

        this.pManager = pManager;
        this.zS = zS;
    }

    public ZookeeperReportService(String zookeeperAddress) {

        zS = new ZookeeperService(zookeeperAddress,
                ZOOKEEPER_CONNECTION_TIME, ZOOKEEPER_SESSION_TIMEOUT, ZOOKEEPER_PATH);
        pManager = new ZookeeperDynamicPropertyManager(zS, ZOOKEEPER_PATH);
    }

    @Override
    public String getTaskProgress(String taskId) throws AccessDeniedOrObjectDoesNotExistException {

        return getProgress(taskId);
    }


    @Override
    public String getDetailedTaskReportBetweenChunks(String taskId, int from, int to) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void incrTaskProgress(String taskId) {

        incr(taskId, PROGRESS_KEY);
    }

    /**
     * @param taskId where progress refers to
     *               TaskId is also used to get the path where the progress is stored.
     * @return Progress for the specified taskId
     * @throws AccessDeniedOrObjectDoesNotExistException
     */
    private String getProgress(String taskId) throws AccessDeniedOrObjectDoesNotExistException {

        final String key = taskId + "/" + PROGRESS_KEY;
        LOGGER.info("fetching progress for task with key {}", key);

        String currentValue = pManager.getCurrentValue(key);
        if (currentValue == null) {
            throw new AccessDeniedOrObjectDoesNotExistException("Progress for taskId: [" + taskId + "] not found..");
        }

        return currentValue;
    }

    /**
     * Increases the current value of the specified @param metric
     * by 1.
     *
     * @param taskId Metric belongs to this taskId
     *               <p/>
     *               TaskId is used to generate the path where the metric will be stored.
     */
    private void incr(String taskId, String metric) {

        final String key = taskId + "/" + metric;
        String currentValue = pManager.getCurrentValue(key);

        if (currentValue == null) {
            currentValue = "0";
        }

        int c = Integer.parseInt(currentValue);
        c += 1;

        pManager.updateValue(key, c + "");
    }
}