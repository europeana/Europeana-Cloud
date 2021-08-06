package eu.europeana.cloud.service.dps.storm.spout;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.TaskDiagnosticInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.*;
import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;
import static org.apache.commons.collections.CollectionUtils.isEmpty;

public class ECloudSpout extends KafkaSpout<String, DpsRecord> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ECloudSpout.class);
    private static final int MAX_RETRIES = 3;

    private String topologyName;
    private String hosts;
    private int port;
    private String keyspaceName;
    private String userName;
    private String password;

    protected transient CassandraTaskInfoDAO taskInfoDAO;
    protected transient TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;
    protected transient TaskStatusUpdater taskStatusUpdater;
    protected transient TaskStatusChecker taskStatusChecker;
    protected transient ProcessedRecordsDAO processedRecordsDAO;
    protected transient TasksCache tasksCache;

    public ECloudSpout(String topologyName, KafkaSpoutConfig<String, DpsRecord> kafkaSpoutConfig, String hosts, int port, String keyspaceName,
                       String userName, String password) {
        super(kafkaSpoutConfig);

        this.topologyName=topologyName;
        this.hosts = hosts;
        this.port = port;
        this.keyspaceName = keyspaceName;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public void ack(Object messageId) {
        LOGGER.info("Record acknowledged {}", messageId);
        super.ack(messageId);
    }

    private void ackIgnoredMessage(Object messageId) {
        super.ack(messageId);
    }

    @Override
    public void fail(Object messageId) {
        LOGGER.error("Record failed {}", messageId);
        super.fail(messageId);
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, new ECloudOutputCollector(collector));

        var cassandraConnectionProvider =
                CassandraConnectionProviderSingleton.getCassandraConnectionProvider(
                        this.hosts,
                        this.port,
                        this.keyspaceName,
                        this.userName,
                        this.password);
        taskInfoDAO = CassandraTaskInfoDAO.getInstance(cassandraConnectionProvider);
        taskStatusUpdater = TaskStatusUpdater.getInstance(cassandraConnectionProvider);
        TaskStatusChecker.init(cassandraConnectionProvider);
        taskStatusChecker = TaskStatusChecker.getTaskStatusChecker();
        processedRecordsDAO = ProcessedRecordsDAO.getInstance(cassandraConnectionProvider);
        taskDiagnosticInfoDAO = TaskDiagnosticInfoDAO.getInstance(cassandraConnectionProvider);
        tasksCache = new TasksCache(cassandraConnectionProvider);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(StormTaskTuple.getFields());
        declarer.declareStream(NOTIFICATION_STREAM_NAME, NotificationTuple.getFields());
    }

    public class ECloudOutputCollector extends SpoutOutputCollector {

        public ECloudOutputCollector(ISpoutOutputCollector delegate) {
            super(delegate);
        }

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
            DpsRecord message = null;
            try {
                message = readMessageFromTuple(tuple);

                if (taskStatusChecker.hasDroppedStatus(message.getTaskId())) {
                    return omitMessageFromDroppedTask(message, messageId);
                }

                ProcessedRecord aRecord = prepareRecordForExecution(message);
                if (isFinished(aRecord)) {
                    return omitAlreadyProcessedRecord(message, messageId);
                }

                if (maxTriesReached(aRecord)) {
                    return emitMaxTriesReachedNotification(message, messageId);
                } else {
                    return emitRecordForProcessing(streamId, message, aRecord, messageId);
                }
            } catch (IOException | NullPointerException e) {
                LOGGER.error("Unable to read message", e);
                return Collections.emptyList();
            } catch (TaskInfoDoesNotExistException e) {
                LOGGER.error("Task definition not found in DB for: {}", message);
                return Collections.emptyList();
            }
        }

        List<Integer> omitAlreadyProcessedRecord(DpsRecord message, Object messageId) {
            //Ignore records that is already preformed. It could take place after spout restart
            //if record was performed but was not acknowledged in kafka service. It is normal situation.
            //Kafka messages can be accepted in sequential order, but storm performs record in parallel so some
            //records must wait for ack before previous records will be confirmed. If spout is stopped in meantime,
            //unconfirmed but completed records would be unnecessary repeated when spout will start next time.
            LOGGER.info("Dropping kafka message for task {} because record {} was already processed: ", message.getTaskId(), message.getRecordId());
            ackIgnoredMessage(messageId);
            return Collections.emptyList();
        }

        List<Integer> emitRecordForProcessing(String streamId, DpsRecord message, ProcessedRecord aRecord, Object compositeMessageId) throws TaskInfoDoesNotExistException, IOException {
            var taskInfo = getTaskInfo(message);
            updateDiagnosticCounters(aRecord);
            var stormTaskTuple = prepareTaskForEmission(taskInfo, message, aRecord);
            LOGGER.info("Emitting a record to the subsequent bolt: {}", message);
            return super.emit(streamId, stormTaskTuple.toStormTuple(), compositeMessageId);
        }

        List<Integer> emitMaxTriesReachedNotification(DpsRecord message, Object compositeMessageId) {
            LOGGER.info("Emitting record to the notification bolt directly because of max_retries reached: {}", message);
            var notificationTuple = NotificationTuple.prepareNotification(
                    message.getTaskId(),
                    message.isMarkedAsDeleted(),
                    message.getRecordId(),
                    RecordState.ERROR,
                    "Max retries reached",
                    "Max retries reached",
                    System.currentTimeMillis());
            return super.emit(NOTIFICATION_STREAM_NAME, notificationTuple.toStormTuple(), compositeMessageId);
        }

        List<Integer> omitMessageFromDroppedTask(DpsRecord message, Object messageId) {
            // Ignores message from dropped tasks. Such message should not be emitted,
            // but must be acknowledged to not remain in topic and to allow acknowledgement
            // of any following messages.
            ackIgnoredMessage(messageId);
            LOGGER.info("Dropping kafka message because task was dropped: {}", message.getTaskId());
            return Collections.emptyList();
        }

        private boolean maxTriesReached(ProcessedRecord aRecord) {
            return aRecord.getAttemptNumber() > MAX_RETRIES;
        }

        private boolean isFinished(ProcessedRecord aRecord) {
            return aRecord.getState() == RecordState.SUCCESS || aRecord.getState() == RecordState.ERROR;
        }

        private DpsRecord readMessageFromTuple(List<Object> tuple){
            return (DpsRecord)tuple.get(4);
        }

        private TaskInfo getTaskInfo(DpsRecord message) throws TaskInfoDoesNotExistException {
            return tasksCache.getTaskInfo(message);
        }

        private StormTaskTuple prepareTaskForEmission(TaskInfo taskInfo, DpsRecord dpsRecord, ProcessedRecord aRecord) throws IOException {
            //
            var dpsTask = DpsTask.fromTaskInfo(taskInfo);
            var stormTaskTuple = new StormTaskTuple(
                    dpsTask.getTaskId(),
                    dpsTask.getTaskName(),
                    dpsRecord.getRecordId(),
                    null,
                    dpsTask.getParameters(),
                    dpsTask.getOutputRevision(),
                    dpsTask.getHarvestingDetails());
            //
            stormTaskTuple.addParameter(CLOUD_LOCAL_IDENTIFIER, dpsRecord.getRecordId());
            stormTaskTuple.addParameter(SCHEMA_NAME, dpsRecord.getMetadataPrefix());
            stormTaskTuple.addParameter(SENT_DATE, DateHelper.format(taskInfo.getSentTimestamp()));
            stormTaskTuple.addParameter(MESSAGE_PROCESSING_START_TIME_IN_MS, new Date().getTime() + "");

            List<String> repositoryUrlList = dpsTask.getDataEntry(InputDataType.REPOSITORY_URLS);
            if (!isEmpty(repositoryUrlList)) {
                stormTaskTuple.addParameter(DPS_TASK_INPUT_DATA, repositoryUrlList.get(0));
            }

            //Implementation of re-try mechanism after topology broken down
            stormTaskTuple.setRecordAttemptNumber(aRecord.getAttemptNumber());

            stormTaskTuple.setMarkedAsDeleted(dpsRecord.isMarkedAsDeleted());

            return stormTaskTuple;
        }

        private void updateDiagnosticCounters(ProcessedRecord aRecord) {
            TaskDiagnosticInfo taskInfo = tasksCache.getDiagnosticInfo(aRecord.getTaskId());

            if (taskInfo.getStartedCount() == 0) {
                taskInfo.setStartOnStormTime(Instant.now());
                taskDiagnosticInfoDAO.updateStartOnStormTime(taskInfo.getId(), taskInfo.getStartOnStormTime());
                taskDiagnosticInfoDAO.updateRecordsRetryCount(taskInfo.getId(), 0);
            }

            if (aRecord.getAttemptNumber() > 1) {
                LOGGER.info("Task {} the record {} is repeated - {} attempt!", taskInfo.getId(), aRecord.getRecordId(), aRecord.getAttemptNumber());
                int retryCount = taskInfo.getRecordsRetryCount();
                retryCount++;
                taskInfo.setRecordsRetryCount(retryCount);
                taskDiagnosticInfoDAO.updateRecordsRetryCount(taskInfo.getId(), retryCount);
            } else {
                taskInfo.setStartedCount(taskInfo.getStartedCount() + 1);
                taskDiagnosticInfoDAO.updateStartedRecordsCount(taskInfo.getId(), taskInfo.getStartedCount());
            }
        }

        private ProcessedRecord prepareRecordForExecution(DpsRecord message) {
            ProcessedRecord aRecord;
            Optional<ProcessedRecord> recordInDb = processedRecordsDAO.selectByPrimaryKey(message.getTaskId(), message.getRecordId());
            if (recordInDb.isPresent()) {
                aRecord = recordInDb.get();
                aRecord.setAttemptNumber(aRecord.getAttemptNumber() + 1);
                processedRecordsDAO.updateAttempNumber(aRecord.getTaskId(), aRecord.getRecordId(), aRecord.getAttemptNumber());
            } else {
                aRecord = ProcessedRecord.builder()
                        .taskId(message.getTaskId())
                        .recordId(message.getRecordId())
                        .attemptNumber(1)
                        .state(RecordState.QUEUED)
                        .topologyName(topologyName)
                        .build();
                processedRecordsDAO.insert(aRecord);
            }
            return aRecord;
        }
    }
}
