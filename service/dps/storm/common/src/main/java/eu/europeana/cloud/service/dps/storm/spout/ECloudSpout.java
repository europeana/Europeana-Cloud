package eu.europeana.cloud.service.dps.storm.spout;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.*;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

        CassandraConnectionProvider cassandraConnectionProvider =
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

                if (taskStatusChecker.hasKillFlag(message.getTaskId())) {
                    return omitMessageFromDroppedTask(message, messageId);
                }

                ProcessedRecord record = prepareRecordForExecution(message);
                if (isFinished(record)) {
                    return omitAlreadyProcessedRecord(message, messageId);
                }

                if (maxTriesReached(record)) {
                    return emitMaxTriesReachedNotification(message, messageId);
                } else {
                    return emitRecordForProcessing(streamId, message, record, messageId);
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

        List<Integer> emitRecordForProcessing(String streamId, DpsRecord message, ProcessedRecord record, Object compositeMessageId) throws TaskInfoDoesNotExistException, IOException {
            TaskInfo taskInfo = getTaskInfo(message);
            updateRetryCount(taskInfo, record);
            StormTaskTuple stormTaskTuple = prepareTaskForEmission(taskInfo, message, record);
            LOGGER.info("Emitting record to the subsequent bolt: {}", message);
            return super.emit(streamId, stormTaskTuple.toStormTuple(), compositeMessageId);
        }

        List<Integer> emitMaxTriesReachedNotification(DpsRecord message, Object compositeMessageId) {
            LOGGER.info("Emitting record to the notification bolt directly because of max_retries reached: {}", message);
            NotificationTuple notificationTuple = NotificationTuple.prepareNotification(
                    message.getTaskId(),
                    message.getRecordId(),
                    RecordState.ERROR,
                    "Max retries reached",
                    "Max retries reached",
                    System.currentTimeMillis());
            notificationTuple.setMarkedAsDeleted(message.isMarkedAsDeleted());
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

        private boolean maxTriesReached(ProcessedRecord record) {
            return record.getAttemptNumber() > MAX_RETRIES;
        }

        private boolean isFinished(ProcessedRecord record) {
            return record.getState() == RecordState.SUCCESS || record.getState() == RecordState.ERROR;
        }

        private DpsRecord readMessageFromTuple(List<Object> tuple){
            return (DpsRecord)tuple.get(4);
        }

        private TaskInfo getTaskInfo(DpsRecord message) throws TaskInfoDoesNotExistException {
            return tasksCache.getTaskInfo(message);
        }

        private StormTaskTuple prepareTaskForEmission(TaskInfo taskInfo, DpsRecord dpsRecord, ProcessedRecord record) throws IOException {
            //
            DpsTask dpsTask = new ObjectMapper().readValue(taskInfo.getTaskDefinition(), DpsTask.class);
            StormTaskTuple stormTaskTuple = new StormTaskTuple(
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
            stormTaskTuple.addParameter(MESSAGE_PROCESSING_START_TIME_IN_MS, new Date().getTime() + "");

            List<String> repositoryUrlList = dpsTask.getDataEntry(InputDataType.REPOSITORY_URLS);
            if (!isEmpty(repositoryUrlList)) {
                stormTaskTuple.addParameter(DPS_TASK_INPUT_DATA, repositoryUrlList.get(0));
            }

            //Implementation of re-try mechanism after topology broken down
            stormTaskTuple.setRecordAttemptNumber(record.getAttemptNumber());

            stormTaskTuple.setMarkedAsDeleted(dpsRecord.isMarkedAsDeleted());

            return stormTaskTuple;
        }

        private void updateRetryCount(TaskInfo taskInfo, ProcessedRecord record) {
            if (record.getAttemptNumber() > 1) {
                LOGGER.info("Task {} record {} is repeated - {} attempt!", taskInfo.getId(), record.getRecordId(), record.getAttemptNumber());
                int retryCount = taskInfo.getRetryCount();
                retryCount++;
                taskInfo.setRetryCount(retryCount);
                taskStatusUpdater.updateRetryCount(taskInfo.getId(), retryCount);
            }
        }

        private ProcessedRecord prepareRecordForExecution(DpsRecord message) {
            ProcessedRecord record;
            Optional<ProcessedRecord> recordInDb = processedRecordsDAO.selectByPrimaryKey(message.getTaskId(), message.getRecordId());
            if (recordInDb.isPresent()) {
                record = recordInDb.get();
                record.setAttemptNumber(record.getAttemptNumber() + 1);
                processedRecordsDAO.updateAttempNumber(record.getTaskId(), record.getRecordId(), record.getAttemptNumber());
            } else {
                record = ProcessedRecord.builder()
                        .taskId(message.getTaskId())
                        .recordId(message.getRecordId())
                        .attemptNumber(1)
                        .state(RecordState.QUEUED)
                        .topologyName(topologyName)
                        .build();
                processedRecordsDAO.insert(record);
            }
            return record;
        }
    }
}
