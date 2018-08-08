package eu.europeana.cloud.service.dps.storm;


import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.utils.CassandraResourceProgressDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraSubTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.util.LRUCache;
import org.apache.commons.lang3.Validate;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * This bolt is responsible for store notifications to Cassandra.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class NotificationBolt extends BaseRichBolt {
    private static final Logger LOGGER = LoggerFactory
            .getLogger(NotificationBolt.class);
    protected Map stormConfig;
    protected TopologyContext topologyContext;
    protected OutputCollector outputCollector;

    private final String hosts;
    private final int port;
    private final String keyspaceName;
    private final String userName;
    private final String password;
    private LRUCache<Long, NotificationCache> cache = new LRUCache<Long, NotificationCache>(
            50);

    private String topologyName;
    private static CassandraConnectionProvider cassandraConnectionProvider;
    private static CassandraTaskInfoDAO taskInfoDAO;
    private static CassandraSubTaskInfoDAO subTaskInfoDAO;
    private static CassandraTaskErrorsDAO taskErrorDAO;
    private static CassandraResourceProgressDAO cassandraResourceProgressDAO;
    private static final int PROCESSED_INTERVAL = 100;

    private static final int DEFAULT_RETRIES = 10;

    private static final int SLEEP_TIME = 5000;

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
    public NotificationBolt(String hosts, int port, String keyspaceName,
                            String userName, String password) {
        this.hosts = hosts;
        this.port = port;
        this.keyspaceName = keyspaceName;
        this.userName = userName;
        this.password = password;

    }

    @Override
    public void execute(Tuple tuple) {
        try {
            NotificationTuple notificationTuple = NotificationTuple
                    .fromStormTuple(tuple);
            NotificationCache nCache = cache.get(notificationTuple.getTaskId());
            if (nCache == null) {
                nCache = getNotificationCache(notificationTuple);
                cache.put(notificationTuple.getTaskId(), nCache);
            }
            storeTaskDetails(notificationTuple, nCache);

        } catch (NoHostAvailableException | QueryExecutionException ex) {
            LOGGER.error("Cannot store notification to Cassandra because: {}",
                    ex.getMessage());
            return;
        } catch (Exception ex) {
            LOGGER.error("Problem with store notification because: {}",
                    ex.getMessage());
            return;
        } finally {
            outputCollector.ack(tuple);
        }
    }

    private NotificationCache getNotificationCache(NotificationTuple notificationTuple) {
        NotificationCache nCache = new NotificationCache();
        try {
            int processed = subTaskInfoDAO.getProcessedFilesCount(notificationTuple.getTaskId());
            if (processed > 0) {
                TaskInfo taskInfo = taskInfoDAO.searchById(notificationTuple.getTaskId());
                LOGGER.info("Recover after failure. The total number of errors is {} , and the total number of processed files {} ", taskInfo.getErrors(), processed);
                nCache.errors = taskInfo.getErrors();
                nCache.processed = processed;
            }

        } catch (TaskInfoDoesNotExistException e) {
            LOGGER.info("This is a new Task {}", e.getMessage());
        }
        return nCache;
    }
    private void storeTaskDetails(NotificationTuple notificationTuple, NotificationCache nCache) throws TaskInfoDoesNotExistException, DatabaseConnectionException {
        long taskId = notificationTuple.getTaskId();
        switch (notificationTuple.getInformationType()) {
            case UPDATE_TASK:
                updateTask(taskId,
                        notificationTuple.getParameters());
                break;
            case NOTIFICATION:
                notifyTask(notificationTuple, nCache, taskId);
                storeFinishState(notificationTuple.getTaskId());
                break;
        }
    }

    private void notifyTask(NotificationTuple notificationTuple, NotificationCache nCache, long taskId) throws DatabaseConnectionException, TaskInfoDoesNotExistException {
        boolean error = isError(String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.STATE)));
        nCache.inc(error);
        int processesFilesCount = nCache.getProcessed();
        int errors = nCache.getErrors();

        storeNotification(processesFilesCount, taskId,
                notificationTuple.getParameters());

        if (error) {
            storeNotificationError(taskId, nCache, notificationTuple.getParameters());
        }
        if ((processesFilesCount % PROCESSED_INTERVAL) == 0) {
            taskInfoDAO.setUpdateProcessedFiles(taskId, processesFilesCount, errors);
        }
    }

    private void storeNotificationError(long taskId, NotificationCache nCache, Map<String, Object> parameters) {
        Validate.notNull(parameters);
        String errorMessage = String.valueOf(parameters.get(NotificationParameterKeys.INFO_TEXT));
        String additionalInformation = String.valueOf(parameters.get(NotificationParameterKeys.ADDITIONAL_INFORMATIONS));
        String errorType = nCache.getErrorType(errorMessage);
        String resource = String.valueOf(parameters.get(NotificationParameterKeys.RESOURCE));
        updateErrorCounter(taskId, errorType);
        insertError(taskId, errorMessage, additionalInformation, errorType, resource);
    }

    private void updateErrorCounter(long taskId, String errorType) {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                taskErrorDAO.updateErrorCounter(taskId, errorType);
                break;
            } catch (Exception e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while updating Error counter. Retries left: {}", retries);
                    try {
                        Thread.sleep(SLEEP_TIME);
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                        LOGGER.error(e1.getMessage());
                    }
                } else {
                    LOGGER.error("Error while updating Error counter.");
                    throw e;
                }
            }
        }
    }

    private void insertError(long taskId, String errorMessage, String additionalInformation, String errorType, String resource) {
        int retries = DEFAULT_RETRIES;

        while (true) {
            try {
                taskErrorDAO.insertError(taskId, errorType, errorMessage, resource, additionalInformation);
                break;
            } catch (Exception e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while inserting Error to cassandra. Retries left: {}", retries);
                    try {
                        Thread.sleep(SLEEP_TIME);
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                        LOGGER.error(e1.getMessage());
                    }
                } else {
                    LOGGER.error("Error while inserting Error to cassandra.");
                    throw e;
                }
            }
        }
    }


    private boolean isError(String state) {
        return state.equalsIgnoreCase(States.ERROR.toString());
    }

    @Override
    public void prepare(Map stormConf, TopologyContext tc, OutputCollector oc) {
        cassandraConnectionProvider = CassandraConnectionProviderSingleton.getCassandraConnectionProvider(hosts, port, keyspaceName,
                userName, password);
        taskInfoDAO = CassandraTaskInfoDAO.getInstance(cassandraConnectionProvider);
        subTaskInfoDAO = CassandraSubTaskInfoDAO.getInstance(cassandraConnectionProvider);
        taskErrorDAO = CassandraTaskErrorsDAO.getInstance(cassandraConnectionProvider);
        cassandraResourceProgressDAO = CassandraResourceProgressDAO.getInstance(cassandraConnectionProvider);
        topologyName = (String) stormConf.get(Config.TOPOLOGY_NAME);
        this.stormConfig = stormConf;
        this.topologyContext = tc;
        this.outputCollector = oc;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {

    }

    private void updateTask(long taskId, Map<String, Object> parameters) throws DatabaseConnectionException {
        Validate.notNull(parameters);
        String state = String.valueOf(parameters.get(NotificationParameterKeys.TASK_STATE));
        String info = String.valueOf(parameters.get(NotificationParameterKeys.INFO));
        Date startDate = prepareDate(parameters.get(NotificationParameterKeys.START_TIME));
        updateTask(taskId, state, info, startDate);
    }

    private void updateTask(long taskId, String state, String info, Date startDate) {
        int retries = DEFAULT_RETRIES;

        while (true) {
            try {
                taskInfoDAO.updateTask(taskId, info, state, startDate);
                break;
            } catch (Exception e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while Updating the task info. Retries left: {}", retries);
                    try {
                        Thread.sleep(SLEEP_TIME);
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                        LOGGER.error(e1.getMessage());
                    }
                } else {
                    LOGGER.error("Error while Updating the task info.");
                    throw e;
                }
            }
        }
    }

    private static Date prepareDate(Object dateObject) {
        Date date = null;
        if (dateObject instanceof Date)
            return (Date) dateObject;
        return date;
    }

    private void storeFinishState(long taskId) throws TaskInfoDoesNotExistException, DatabaseConnectionException {
        TaskInfo task = taskInfoDAO.searchById(taskId);
        if (task != null) {
            NotificationCache nCache = cache.get(taskId);
            int count = nCache.getProcessed();
            int expectedSize = task.getExpectedSize();
            if (count == expectedSize) {
                taskInfoDAO.endTask(taskId, count, nCache.getErrors(), "Completely processed", String.valueOf(TaskState.PROCESSED), new Date());
            }
        }
    }

    private void storeNotification(int resourceNum, long taskId, Map<String, Object> parameters) throws DatabaseConnectionException {
        Validate.notNull(parameters);
        String resource = String.valueOf(parameters.get(NotificationParameterKeys.RESOURCE));
        String state = String.valueOf(parameters.get(NotificationParameterKeys.STATE));
        String infoText = String.valueOf(parameters.get(NotificationParameterKeys.INFO_TEXT));
        String additionalInfo = String.valueOf(parameters.get(NotificationParameterKeys.ADDITIONAL_INFORMATIONS));
        String resultResource = String.valueOf(parameters.get(NotificationParameterKeys.RESULT_RESOURCE));
        insertRecordDetailedInformation(resourceNum, taskId, resource, state, infoText, additionalInfo, resultResource);
        cassandraResourceProgressDAO.updateResourceProgress(taskId, resource, state);
    }

    private void insertRecordDetailedInformation(int resourceNum, long taskId, String resource, String state, String infoText, String additionalInfo, String resultResource) {
        int retries = DEFAULT_RETRIES;

        while (true) {
            try {
                subTaskInfoDAO.insert(resourceNum, taskId, topologyName, resource, state, infoText, additionalInfo, resultResource);
                break;
            } catch (Exception e) {
                if (retries-- > 0) {
                    LOGGER.warn("Error while inserting detailed record information to cassandra. Retries left: {}", retries);
                    try {
                        Thread.sleep(SLEEP_TIME);
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                        LOGGER.error(e1.getMessage());
                    }
                } else {
                    LOGGER.error("Error while inserting detailed record information to cassandra.");
                    throw e;
                }
            }
        }
    }

    public void clearCache() {
        cache.clear();
    }

    private static class NotificationCache {

        int processed = 0;
        int errors = 0;
        Map<String, String> errorTypes = new HashMap<>();

        NotificationCache() {
        }

        NotificationCache(int processed, int errors) {
            this.processed = processed;
            this.errors = errors;
        }

        public void inc(boolean error) {
            processed++;
            if (error) {
                errors++;
            }
        }

        public int getProcessed() {
            return processed;
        }

        public int getErrors() {
            return errors;
        }

        public String getErrorType(String infoText) {
            String errorType = errorTypes.get(infoText);
            if (errorType == null) {
                errorType = new com.eaio.uuid.UUID().toString();
                errorTypes.put(infoText, errorType);
            }
            return errorType;
        }
    }


}


