package eu.europeana.cloud.service.dps.storm;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.dao.*;
import eu.europeana.cloud.service.dps.storm.notification.NotificationHandlerFactory;
import eu.europeana.cloud.service.dps.storm.notification.NotificationHandlerFactoryForDefaultTasks;
import eu.europeana.cloud.service.dps.storm.notification.NotificationHandlerFactoryForPostprocessinTasks;
import eu.europeana.cloud.service.dps.storm.notification.handler.NotificationTupleHandler;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.util.LRUCache;
import lombok.Getter;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.StandardToStringStyle;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * This bolt is responsible for store notifications to Cassandra.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class NotificationBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationBolt.class);
    private final String hosts;
    private final int port;
    private final String keyspaceName;
    private final String userName;
    private final String password;
    protected transient OutputCollector outputCollector;
    protected LRUCache<Long, NotificationCache> cache = new LRUCache<>(50);

    protected String topologyName;
    protected transient TaskStatusUpdater taskStatusUpdater;
    protected transient ProcessedRecordsDAO processedRecordsDAO;
    protected transient CassandraTaskInfoDAO taskInfoDAO;
    private transient TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;
    private transient CassandraSubTaskInfoDAO subTaskInfoDAO;
    private transient CassandraTaskErrorsDAO taskErrorDAO;

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
            var notificationTuple = NotificationTuple.fromStormTuple(tuple);
            var cachedCounters = readCachedCounters(notificationTuple);
            NotificationTupleHandler handler = prepareNotificationHandler(notificationTuple, cachedCounters);
            handler.handle(notificationTuple, cachedCounters);
        } catch (NoHostAvailableException | QueryExecutionException ex) {
            LOGGER.error("Cannot store notification to Cassandra because: {}", ex.getMessage());
        } catch (Exception ex) {
            LOGGER.error("Problem with store notification because: {}", ex.getMessage(), ex);
        } finally {
            outputCollector.ack(tuple);
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext tc, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;

        var cassandraConnectionProvider =
                CassandraConnectionProviderSingleton.getCassandraConnectionProvider(
                        hosts, port, keyspaceName, userName, password);

        taskInfoDAO = CassandraTaskInfoDAO.getInstance(cassandraConnectionProvider);
        taskDiagnosticInfoDAO = TaskDiagnosticInfoDAO.getInstance(cassandraConnectionProvider);
        taskStatusUpdater = TaskStatusUpdater.getInstance(cassandraConnectionProvider);
        subTaskInfoDAO = CassandraSubTaskInfoDAO.getInstance(cassandraConnectionProvider);
        processedRecordsDAO = ProcessedRecordsDAO.getInstance(cassandraConnectionProvider);
        taskErrorDAO = CassandraTaskErrorsDAO.getInstance(cassandraConnectionProvider);
        topologyName = (String) stormConf.get(Config.TOPOLOGY_NAME);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        //last bolt in all topologies, nothing to declare
    }

    protected boolean needsPostProcessing(NotificationTuple tuple) throws TaskInfoDoesNotExistException, IOException {
        return false;
    }

    protected DpsTask loadDpsTask(NotificationTuple tuple) throws TaskInfoDoesNotExistException, IOException {
        Optional<TaskInfo> taskInfo = taskInfoDAO.findById(tuple.getTaskId());
        String taskDefinition = taskInfo.orElseThrow(TaskInfoDoesNotExistException::new).getDefinition();
        return new ObjectMapper().readValue(taskDefinition, DpsTask.class);
    }

    private NotificationCache readCachedCounters(NotificationTuple notificationTuple) {
        var cachedCounters = cache.get(notificationTuple.getTaskId());
        if (cachedCounters == null) {
            cachedCounters = new NotificationCache(notificationTuple.getTaskId());
            cache.put(notificationTuple.getTaskId(), cachedCounters);
        }
        return cachedCounters;
    }

    private NotificationTupleHandler prepareNotificationHandler(NotificationTuple notificationTuple, NotificationCache cachedCounters) throws TaskInfoDoesNotExistException, IOException {
        if (needsPostProcessing(notificationTuple)) {
            NotificationHandlerFactory factory = new NotificationHandlerFactoryForPostprocessinTasks(
                    processedRecordsDAO,
                    taskDiagnosticInfoDAO,
                    taskStatusUpdater,
                    subTaskInfoDAO,
                    taskErrorDAO,
                    taskInfoDAO
            );
            TaskInfo task = taskInfoDAO.findById(notificationTuple.getTaskId()).orElseThrow(TaskInfoDoesNotExistException::new);
            return factory.provide(notificationTuple, task.getExpectedRecordsNumber(), cachedCounters.processed);
        } else {
            NotificationHandlerFactory factory = new NotificationHandlerFactoryForDefaultTasks(
                    processedRecordsDAO,
                    taskDiagnosticInfoDAO,
                    taskStatusUpdater,
                    subTaskInfoDAO,
                    taskErrorDAO,
                    taskInfoDAO
            );
            TaskInfo task = taskInfoDAO.findById(notificationTuple.getTaskId()).orElseThrow(TaskInfoDoesNotExistException::new);
            return factory.provide(notificationTuple, task.getExpectedRecordsNumber(), cachedCounters.processed);
        }
    }

    @Getter
    public class NotificationCache {

        int processed;
        int processedRecordsCount;
        int ignoredRecordsCount;
        int deletedRecordsCount;
        int processedErrorsCount;
        int deletedErrorsCount;

        Map<String, String> errorTypes = new HashMap<>();

        NotificationCache(long taskId) {
            processed = subTaskInfoDAO.getProcessedFilesCount(taskId);
            if (processed > 0) {
                var taskInfo = taskInfoDAO.findById(taskId).orElseThrow();
                processedRecordsCount = taskInfo.getProcessedRecordsCount();
                ignoredRecordsCount = taskInfo.getIgnoredRecordsCount();
                deletedRecordsCount = taskInfo.getDeletedRecordsCount();
                processedErrorsCount = taskInfo.getProcessedErrorsCount();
                deletedErrorsCount = taskInfo.getDeletedErrorsCount();
                errorTypes = getMessagesUUIDsMap(taskId);
                LOGGER.info("Restored state of NotificationBolt from Cassandra for taskId={} counters={}",
                        taskId, getCountersAsText());
            }
        }

        public void incrementCounters(NotificationTuple notificationTuple) {
            processed++;

            if (notificationTuple.isMarkedAsDeleted()) {
                deletedRecordsCount++;
                if (isErrorTuple(notificationTuple)) {
                    deletedErrorsCount++;
                }
            } else if (notificationTuple.isIgnoredRecord()) {
                if (isErrorTuple(notificationTuple)) {
                    LOGGER.error("Tuple is marked as ignored and error in the same time! It should not occur. Tuple: {}"
                            , notificationTuple);
                    processedRecordsCount++;
                    processedErrorsCount++;
                } else {
                    ignoredRecordsCount++;
                }
            } else {
                processedRecordsCount++;
                if (isErrorTuple(notificationTuple)) {
                    processedErrorsCount++;
                }
            }

        }

        public String getErrorType(String infoText) {
            return errorTypes.computeIfAbsent(infoText,
                    key -> new com.eaio.uuid.UUID().toString());
        }

        public String getCountersAsText() {
            var style = new StandardToStringStyle();
            style.setUseClassName(false);
            style.setUseIdentityHashCode(false);
            style.setContentEnd("");
            style.setContentStart("");
            return new ReflectionToStringBuilder(this, style).setExcludeFieldNames("errorTypes").toString();
        }

        private boolean isErrorTuple(NotificationTuple notificationTuple) {
            return String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.STATE)).equalsIgnoreCase(RecordState.ERROR.toString());
        }

        private Map<String, String> getMessagesUUIDsMap(long taskId) {
            Map<String, String> errorMessageToUuidMap = new HashMap<>();
            Iterator<String> it = taskErrorDAO.getMessagesUuids(taskId);
            while (it.hasNext()) {
                String errorType = it.next();
                Optional<String> message = taskErrorDAO.getErrorMessage(taskId, errorType);
                message.ifPresent(s -> errorMessageToUuidMap.put(s, errorType));
            }
            return errorMessageToUuidMap;
        }
    }
}