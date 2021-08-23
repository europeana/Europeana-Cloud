package eu.europeana.cloud.service.dps.storm;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.*;
import eu.europeana.cloud.service.dps.Constants;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.dao.CassandraSubTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.util.LRUCache;
import lombok.Getter;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.StandardToStringStyle;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

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
            var nCache = cache.get(notificationTuple.getTaskId());
            if (nCache == null) {
                nCache = new NotificationCache(notificationTuple.getTaskId());
                cache.put(notificationTuple.getTaskId(), nCache);
            }
            storeNotificationInfo(notificationTuple, nCache);

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

    private void storeNotificationInfo(NotificationTuple notificationTuple, NotificationCache nCache) throws TaskInfoDoesNotExistException {
        long taskId = notificationTuple.getTaskId();
        var recordId = String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.RESOURCE));
        Optional<ProcessedRecord> theRecord = processedRecordsDAO.selectByPrimaryKey(taskId, recordId);
        if (theRecord.isEmpty() || !isFinished(theRecord.get())) {
            notifyTask(notificationTuple, nCache, taskId);
            RecordState newRecordState = isErrorTuple(notificationTuple) ? RecordState.ERROR : RecordState.SUCCESS;
            processedRecordsDAO.updateProcessedRecordState(taskId, recordId, newRecordState);
            storeFinishState(notificationTuple);
        }
        taskDiagnosticInfoDAO.updateLastRecordFinishedOnStormTime(notificationTuple.getTaskId(), Instant.now());
    }

    private void notifyTask(NotificationTuple notificationTuple, NotificationCache nCache, long taskId) {
        boolean error = isError(notificationTuple);
        nCache.incrementCounters(notificationTuple);

        //notification table (for single record)
        storeNotification(nCache.getProcessed(), taskId,
                notificationTuple.getParameters());

        if (error) {
            storeNotificationError(taskId, nCache, notificationTuple);
        }

        saveProgressCounters(taskId, nCache);
    }

    private void saveProgressCounters(long taskId, NotificationCache nCache) {
        LOGGER.info("Updating task counter for task_id = {} and counters: {}", taskId, nCache.getCountersAsText());
        taskStatusUpdater.setUpdateProcessedFiles(taskId, nCache.getProcessedRecordsCount(),
                nCache.getIgnoredRecordsCount(), nCache.getDeletedRecordsCount(),
                nCache.getProcessedErrorsCount(), nCache.getDeletedErrorsCount());
    }

    private void storeNotificationError(long taskId, NotificationCache nCache, NotificationTuple notificationTuple) {
        Map<String, Object> parameters = notificationTuple.getParameters();
        Validate.notNull(parameters);
        var errorMessage = String.valueOf(parameters.get(NotificationParameterKeys.INFO_TEXT));
        var additionalInformation = String.valueOf(parameters.get(NotificationParameterKeys.ADDITIONAL_INFORMATIONS));
        if (!isErrorTuple(notificationTuple) && parameters.get(PluginParameterKeys.UNIFIED_ERROR_MESSAGE) != null) {
            errorMessage = String.valueOf(parameters.get(NotificationParameterKeys.UNIFIED_ERROR_MESSAGE));
            additionalInformation = String.valueOf(parameters.get(NotificationParameterKeys.EXCEPTION_ERROR_MESSAGE));
        }
        var  errorType = nCache.getErrorType(errorMessage);
        var resource = String.valueOf(parameters.get(NotificationParameterKeys.RESOURCE));
        updateErrorCounter(taskId, errorType);
        insertError(taskId, errorMessage, additionalInformation, errorType, resource);
    }

    private void updateErrorCounter(long taskId, String errorType) {
        taskErrorDAO.updateErrorCounter(taskId, errorType);
    }

    private void insertError(long taskId, String errorMessage, String additionalInformation, String errorType, String resource) {
        long errorCount = taskErrorDAO.selectErrorCountsForErrorType(taskId, UUID.fromString(errorType));
        if (!maximumNumberOfErrorsReached(errorCount)) {
            taskErrorDAO.insertError(taskId, errorType, errorMessage, resource, additionalInformation);
        } else {
            LOGGER.warn("Will not store the error message because threshold reached for taskId={}. ", taskId);
        }
    }

    private boolean maximumNumberOfErrorsReached(long errorCount) {
        return errorCount > Constants.MAXIMUM_ERRORS_THRESHOLD_FOR_ONE_ERROR_TYPE;
    }

    private boolean isError(NotificationTuple notificationTuple) {
        return isErrorTuple(notificationTuple)
                || (notificationTuple.getParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE) != null);
    }

    private void storeFinishState(NotificationTuple notificationTuple) throws TaskInfoDoesNotExistException {
        long taskId = notificationTuple.getTaskId();
        TaskInfo task = taskInfoDAO.findById(taskId).orElseThrow(TaskInfoDoesNotExistException::new);
        if (task != null) {
            NotificationCache nCache = cache.get(taskId);
            int count = nCache.getProcessed();
            int expectedSize = task.getExpectedRecordsNumber();
            if (count == expectedSize) {
                endTask(notificationTuple, nCache);
            }
        }
    }

    private void storeNotification(int resourceNum, long taskId, Map<String, Object> parameters) {
        Validate.notNull(parameters);
        var resource = String.valueOf(parameters.get(NotificationParameterKeys.RESOURCE));
        var state = String.valueOf(parameters.get(NotificationParameterKeys.STATE));
        var infoText = String.valueOf(parameters.get(NotificationParameterKeys.INFO_TEXT));
        var additionalInfo = String.valueOf(parameters.get(NotificationParameterKeys.ADDITIONAL_INFORMATIONS));
        var resultResource = String.valueOf(parameters.get(NotificationParameterKeys.RESULT_RESOURCE));
        var now = Calendar.getInstance().getTimeInMillis();
        var processingTime = now - (Long) parameters.get(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS);
        additionalInfo = additionalInfo + " Processing time: " + processingTime;
        insertRecordDetailedInformation(resourceNum, taskId, resource, state, infoText, additionalInfo, resultResource);
    }

    private boolean isErrorTuple(NotificationTuple notificationTuple) {
        return String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.STATE)).equalsIgnoreCase(RecordState.ERROR.toString());
    }

    private boolean isFinished(ProcessedRecord theRecord) {
        return theRecord.getState() == RecordState.SUCCESS || theRecord.getState() == RecordState.ERROR;
    }

    @Getter
    protected class NotificationCache {

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
    }

    protected void endTask(NotificationTuple notificationTuple, NotificationCache nCache) {
        try {
            if (needsPostProcessing(notificationTuple)) {
                setTaskStatusToReadyForPostprocessing(notificationTuple, nCache);
            } else {
                setTaskProcessed(notificationTuple, nCache);
            }
            taskDiagnosticInfoDAO.updateFinishOnStormTime(notificationTuple.getTaskId(), Instant.now());
        } catch (Exception e) {
            LOGGER.error("Unable to end the task. id: {} ", notificationTuple.getTaskId(), e);
            taskStatusUpdater.setTaskDropped(notificationTuple.getTaskId(), "Unable to end the task");
        }
    }

    protected boolean needsPostProcessing(NotificationTuple tuple) throws TaskInfoDoesNotExistException, IOException {
        return false;
    }

    private void setTaskProcessed(NotificationTuple notificationTuple, NotificationCache nCache) {
        taskStatusUpdater.setTaskCompletelyProcessed(notificationTuple.getTaskId(), "Completely processed");
        LOGGER.info("Task id={} completely processed! Counters: {} ", notificationTuple.getTaskId(),
                nCache.getCountersAsText());
    }

    private void setTaskStatusToReadyForPostprocessing(NotificationTuple notificationTuple, NotificationCache nCache) {
        taskStatusUpdater.updateState(notificationTuple.getTaskId(), TaskState.READY_FOR_POST_PROCESSING,
                "Ready for post processing after topology stage is finished");
        LOGGER.info("Task id={} finished topology stage. Now it is waiting for post processing. Counters: {}",
                notificationTuple.getTaskId(), nCache.getCountersAsText());
    }

    protected DpsTask loadDpsTask(NotificationTuple tuple) throws TaskInfoDoesNotExistException, IOException {
        Optional<TaskInfo> taskInfo = taskInfoDAO.findById(tuple.getTaskId());
        String taskDefinition = taskInfo.orElseThrow(TaskInfoDoesNotExistException::new).getDefinition();
        return new ObjectMapper().readValue(taskDefinition, DpsTask.class);
    }

    protected void insertRecordDetailedInformation(int resourceNum, long taskId, String resource, String state, String infoText, String additionalInfo, String resultResource) {
        subTaskInfoDAO.insert(resourceNum, taskId, topologyName, resource, state, infoText, additionalInfo, resultResource);
    }
}