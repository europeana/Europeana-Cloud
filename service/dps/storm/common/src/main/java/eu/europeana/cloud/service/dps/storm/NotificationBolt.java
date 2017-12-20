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

import java.util.*;

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
    private static LRUCache<Long, NotificationCache> cache = new LRUCache<Long, NotificationCache>(
            100);

    private String topologyName;
    private static CassandraConnectionProvider cassandraConnectionProvider;
    private static CassandraTaskInfoDAO taskInfoDAO;
    private static CassandraSubTaskInfoDAO subTaskInfoDAO;
    private static CassandraTaskErrorsDAO taskErrorDAO;
    private static final int PROCESSED_INTERVAL = 100;

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
                nCache = new NotificationCache();
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

    private void storeTaskDetails(NotificationTuple notificationTuple, NotificationCache nCache) throws TaskInfoDoesNotExistException, DatabaseConnectionException {
        long taskId = notificationTuple.getTaskId();
        switch (notificationTuple.getInformationType()) {
            case UPDATE_TASK:
                updateTask(taskId,
                        notificationTuple.getParameters());
                break;
            case END_TASK:
                endTask(taskId, nCache.getProcessed(), nCache.getErrors(),
                        notificationTuple.getParameters());
                break;
            case NOTIFICATION:
                notifyTask(notificationTuple, nCache, taskId);
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
        String errorType = nCache.getErrorType(errorMessage);
        String resource = String.valueOf(parameters.get(NotificationParameterKeys.RESOURCE));
        taskErrorDAO.updateErrorCounter(taskId, errorType);
        taskErrorDAO.insertError(taskId, errorType, errorMessage, resource);
    }

    private boolean isError(String state) {
        return state.equalsIgnoreCase(States.ERROR.toString()) || state.equalsIgnoreCase(States.DROPPED.toString());
    }

    @Override
    public void prepare(Map stormConf, TopologyContext tc, OutputCollector oc) {
        cassandraConnectionProvider = CassandraConnectionProviderSingleton.getCassandraConnectionProvider(hosts, port, keyspaceName,
                userName, password);
        taskInfoDAO = CassandraTaskInfoDAO.getInstance(cassandraConnectionProvider);
        subTaskInfoDAO = CassandraSubTaskInfoDAO.getInstance(cassandraConnectionProvider);
        taskErrorDAO = CassandraTaskErrorsDAO.getInstance(cassandraConnectionProvider);
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
        taskInfoDAO.updateTask(taskId, info, state, startDate);
    }


    private void endTask(long taskId, int processeFilesCount, int errors, Map<String, Object> parameters) throws DatabaseConnectionException {
        Validate.notNull(parameters);
        String state = String.valueOf(parameters.get(NotificationParameterKeys.TASK_STATE));
        Date finishDate = prepareDate(parameters.get(NotificationParameterKeys.FINISH_TIME));
        String info = String.valueOf(parameters.get(NotificationParameterKeys.INFO));
        taskInfoDAO.endTask(taskId, processeFilesCount, errors, info, state, finishDate);
    }


    private static Date prepareDate(Object dateObject) {
        Date date = null;
        if (dateObject instanceof Date)
            return (Date) dateObject;
        return date;
    }

    private void storeNotification(int resourceNum, long taskId, Map<String, Object> parameters) throws DatabaseConnectionException {
        Validate.notNull(parameters);
        String resource = String.valueOf(parameters.get(NotificationParameterKeys.RESOURCE));
        String state = String.valueOf(parameters.get(NotificationParameterKeys.STATE));
        String infoText = String.valueOf(parameters.get(NotificationParameterKeys.INFO_TEXT));
        String additionalInfo = String.valueOf(parameters.get(NotificationParameterKeys.ADDITIONAL_INFORMATIONS));
        String resultResource = String.valueOf(parameters.get(NotificationParameterKeys.RESULT_RESOURCE));
        subTaskInfoDAO.insert(resourceNum, taskId, topologyName, resource, state, infoText, additionalInfo, resultResource);
    }

    private static class NotificationCache {

        int processed = 0;
        int errors = 0;
        Map<String, String> errorTypes = new HashMap<>();

        NotificationCache() {
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

        public int getErrors() { return errors; }

        public String getErrorType(String infoText) {
            String errorType = errorTypes.get(infoText);
            if (errorType == null) {
                errorType = new com.eaio.uuid.UUID().toString();
                errorTypes.put(infoText, errorType);
            }
            return errorType;
        }
    }

    public static void clearCache() {
        cache.clear();
    }

}


