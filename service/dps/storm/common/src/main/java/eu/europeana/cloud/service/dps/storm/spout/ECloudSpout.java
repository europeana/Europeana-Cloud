package eu.europeana.cloud.service.dps.storm.spout;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.TaskDiagnosticInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.TaskDiagnosticInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.DiagnosticContextWrapper;
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

import javax.management.*;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.*;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.*;
import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;
import static org.apache.commons.collections.CollectionUtils.isEmpty;

public class ECloudSpout extends KafkaSpout<String, DpsRecord> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ECloudSpout.class);
  private static final int MAX_RETRIES = 3;
  private final String topologyName;
  private final String topic;
  private final String hosts;
  private final int port;
  private final String keyspaceName;
  private final String userName;
  private final String password;
  protected transient CassandraTaskInfoDAO taskInfoDAO;
  protected transient TaskDiagnosticInfoDAO taskDiagnosticInfoDAO;
  protected transient TaskStatusUpdater taskStatusUpdater;
  protected transient TaskStatusChecker taskStatusChecker;
  protected transient ProcessedRecordsDAO processedRecordsDAO;
  protected transient TasksCache tasksCache;
  protected transient ECloudSpoutSamplerMXBean eCloudSpoutSamplerMXBean;
  private transient ECloudOutputCollector eCloudOutputCollector;
  protected long maxTaskPending = Long.MAX_VALUE;

  public ECloudSpout(String topologyName, String topic, KafkaSpoutConfig<String, DpsRecord> kafkaSpoutConfig, CassandraProperties cassandraProperties) {
    super(kafkaSpoutConfig);
    this.topologyName = topologyName;
    this.topic = topic;
    this.hosts = cassandraProperties.getHosts();
    this.port = cassandraProperties.getPort();
    this.keyspaceName = cassandraProperties.getKeyspace();
    this.userName = cassandraProperties.getUser();
    this.password = cassandraProperties.getPassword();
  }


  @Override
  public void ack(Object messageId) {
    eCloudSpoutSamplerMXBean.lastAckedMessageId = String.valueOf(messageId);
    LOGGER.info("Record acknowledged {}", messageId);
    super.ack(messageId);
  }

  @Override
  public void fail(Object messageId) {
    eCloudSpoutSamplerMXBean.lastFailedMessageId = String.valueOf(messageId);
    LOGGER.error("Record failed {}", messageId);
    super.fail(messageId);
  }

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    eCloudSpoutSamplerMXBean = new ECloudSpoutSamplerMXBean();
    eCloudOutputCollector = new ECloudOutputCollector(collector);
    super.open(conf, context, eCloudOutputCollector);

    var cassandraConnectionProvider =
        CassandraConnectionProviderSingleton.getCassandraConnectionProvider(
            this.hosts,
            this.port,
            this.keyspaceName,
            this.userName,
            this.password);
    taskInfoDAO = CassandraTaskInfoDAO.getInstance(cassandraConnectionProvider);
    taskStatusUpdater = TaskStatusUpdater.getInstance(cassandraConnectionProvider);
    taskStatusChecker = TaskStatusChecker.getTaskStatusChecker(cassandraConnectionProvider);
    processedRecordsDAO = ProcessedRecordsDAO.getInstance(cassandraConnectionProvider);
    taskDiagnosticInfoDAO = TaskDiagnosticInfoDAO.getInstance(cassandraConnectionProvider);
    tasksCache = new TasksCache(cassandraConnectionProvider);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(StormTaskTuple.getFields());
    declarer.declareStream(NOTIFICATION_STREAM_NAME, NotificationTuple.getFields());
  }

  private void ackIgnoredMessage(Object messageId) {
    super.ack(messageId);
  }

  private StormTaskTuple getStormTaskTupleFromMessage(DpsRecord message) {
    StormTaskTuple stormTaskTuple = new StormTaskTuple();
    stormTaskTuple.setTaskId(message.getTaskId());
    stormTaskTuple.setMarkedAsDeleted(message.isMarkedAsDeleted());
    stormTaskTuple.setFileUrl(message.getRecordId());
    stormTaskTuple.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, String.valueOf(System.currentTimeMillis()));
    return stormTaskTuple;
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
        eCloudSpoutSamplerMXBean.lastConsumedMessageId = String.valueOf(messageId);
        eCloudSpoutSamplerMXBean.lastConsumedMessage = String.valueOf(message);
        DiagnosticContextWrapper.putValuesFrom(message);

        LOGGER.info("Reading message from Queue");
        if (taskStatusChecker.hasDroppedStatus(message.getTaskId())) {
          eCloudSpoutSamplerMXBean.lastConsumedMessageCanceled = true;
          return omitMessageFromDroppedTask(messageId);
        }
        eCloudSpoutSamplerMXBean.lastConsumedMessageCanceled = false;

        ProcessedRecord aRecord = prepareRecordForExecution(message);
        if (isFinished(aRecord)) {
          return omitAlreadyProcessedRecord(messageId);
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
        LOGGER.error("Task definition not found in DB");
        return Collections.emptyList();
      } finally {
        DiagnosticContextWrapper.clear();
      }
    }

    private boolean maxTriesReached(ProcessedRecord aRecord) {
      return aRecord.getAttemptNumber() > MAX_RETRIES;
    }

    private boolean isFinished(ProcessedRecord aRecord) {
      return aRecord.getState() == RecordState.SUCCESS || aRecord.getState() == RecordState.ERROR;
    }

    private DpsRecord readMessageFromTuple(List<Object> tuple) {
      return (DpsRecord) tuple.get(4);
    }

    private TaskInfo getTaskInfo(DpsRecord message) throws TaskInfoDoesNotExistException {
      return tasksCache.getTaskInfo(message);
    }

    private StormTaskTuple prepareTaskForEmission(TaskInfo taskInfo, DpsTask dpsTask, DpsRecord dpsRecord,
        ProcessedRecord aRecord) {
      //
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
      List<String> datasetUrlList = dpsTask.getDataEntry(InputDataType.DATASET_URLS);
      if (!isEmpty(datasetUrlList)){
        stormTaskTuple.addParameter(DPS_TASK_INPUT_DATA, datasetUrlList.get(0));
      }

      //Implementation of re-try mechanism after topology broken down
      stormTaskTuple.setRecordAttemptNumber(aRecord.getAttemptNumber());

      stormTaskTuple.setMarkedAsDeleted(dpsRecord.isMarkedAsDeleted());

      return stormTaskTuple;
    }

    private void updateDiagnosticCounters(ProcessedRecord aRecord) {
      TaskDiagnosticInfo taskInfo = tasksCache.getDiagnosticInfo(aRecord.getTaskId());

      if (taskInfo.getStartedRecordsCount() == 0) {
        taskInfo.setStartOnStormTime(Instant.now());
        taskDiagnosticInfoDAO.updateStartOnStormTime(taskInfo.getTaskId(), taskInfo.getStartOnStormTime());
        taskDiagnosticInfoDAO.updateRecordsRetryCount(taskInfo.getTaskId(), 0);
      }

      if (aRecord.getAttemptNumber() > 1) {
        LOGGER.info("Record is repeated - {} attempt!", aRecord.getAttemptNumber());
        int retryCount = taskInfo.getRecordsRetryCount();
        retryCount++;
        taskInfo.setRecordsRetryCount(retryCount);
        taskDiagnosticInfoDAO.updateRecordsRetryCount(taskInfo.getTaskId(), retryCount);
      } else {
        taskInfo.setStartedRecordsCount(taskInfo.getStartedRecordsCount() + 1);
        taskDiagnosticInfoDAO.updateStartedRecordsCount(taskInfo.getTaskId(), taskInfo.getStartedRecordsCount());
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

    List<Integer> omitAlreadyProcessedRecord(Object messageId) {
      //Ignore records that is already preformed. It could take place after spout restart
      //if record was performed but was not acknowledged in kafka service. It is normal situation.
      //Kafka messages can be accepted in sequential order, but storm performs record in parallel so some
      //records must wait for ack before previous records will be confirmed. If spout is stopped in meantime,
      //unconfirmed but completed records would be unnecessary repeated when spout will start next time.
      LOGGER.info("Dropping kafka message because record was already processed");
      ackIgnoredMessage(messageId);
      return Collections.emptyList();
    }

    List<Integer> emitRecordForProcessing(String streamId, DpsRecord message, ProcessedRecord aRecord,
        Object compositeMessageId) throws TaskInfoDoesNotExistException, IOException {
      var taskInfo = getTaskInfo(message);
      var dpsTask = DpsTask.fromTaskInfo(taskInfo);
      updateDiagnosticCounters(aRecord);
      var stormTaskTuple = prepareTaskForEmission(taskInfo, dpsTask, message, aRecord);
      performThrottling(stormTaskTuple);
      LOGGER.info("Emitting a record to the subsequent bolt maxPending: {}", maxTaskPending);
      return super.emit(streamId, stormTaskTuple.toStormTuple(), compositeMessageId);
    }

    List<Integer> emitMaxTriesReachedNotification(DpsRecord message, Object compositeMessageId) {
      LOGGER.info("Emitting record to the notification bolt directly because of max_retries reached");
      StormTaskTuple stormTaskTuple = getStormTaskTupleFromMessage(message);
      var notificationTuple = NotificationTuple.prepareNotification(
              stormTaskTuple,
              RecordState.ERROR,
              "Max retries reached",
              "Max retries reached"
      );
      return super.emit(NOTIFICATION_STREAM_NAME, notificationTuple.toStormTuple(), compositeMessageId);
    }

    List<Integer> omitMessageFromDroppedTask(Object messageId) {
      // Ignores message from dropped tasks. Such message should not be emitted,
      // but must be acknowledged to not remain in topic and to allow acknowledgement
      // of any following messages.
      ackIgnoredMessage(messageId);
      LOGGER.info("Dropping kafka message because task was dropped");
      return Collections.emptyList();
    }
  }

  /**
   * MX bean responsible for sampling spout used by the topologies
   */
  private class ECloudSpoutSamplerMXBean implements ECloudSpoutMXBean {

    protected transient String lastConsumedMessageId;
    protected transient String lastConsumedMessage;
    protected transient String lastAckedMessageId;
    protected transient String lastFailedMessageId;
    protected transient boolean lastConsumedMessageCanceled;

    public ECloudSpoutSamplerMXBean() {
      register();
    }

    public void register() {
      try {
        ManagementFactory.getPlatformMBeanServer().registerMBean(this, new ObjectName(this.getName()));
      } catch (NotCompliantMBeanException | InstanceAlreadyExistsException | MalformedObjectNameException
               | MBeanRegistrationException e) {
        throw new RuntimeException(e);
      }
    }

    public String getName() {
      return "eu.europeana.cloud:executor=spouts,topic=" + topic;
    }

    public String getLastConsumedMessageId() {
      return lastConsumedMessageId;
    }

    public String getLastConsumedMessage() {
      return lastConsumedMessage;
    }

    public String getLastAckedMessageId() {
      return lastAckedMessageId;
    }

    public String getLastFailedMessageId() {
      return lastFailedMessageId;
    }

    @Override
    public long getMaxSpoutPending() {
      return maxTaskPending;
    }

    @Override
    public String showSpoutToString() {
      return getSpoutString();
    }

    @Override
    public String showOffsetManagers() {
      String s = getSpoutString();
      int index = s.lastIndexOf("emitted=");
      if (index > -1) {
        return s.substring(0, index)
                .replaceAll("ackedMsgs=", "\nackedMsgs=\n")
                .replaceAll("},", "},\n");
      } else {
        return "Could not find info in spout string!";
      }

    }

    @Override
    public String showEmitted() {
      String s = getSpoutString();
      int index = s.lastIndexOf("emitted=");
      if (index > -1) {
        return s.substring(index)
                .replaceAll("},", "},\n");
      } else {
        return "Could not find info in spout string!";
      }
    }

    private String getSpoutString() {
      return ECloudSpout.this.toString();
    }
  }

  protected void performThrottling(StormTaskTuple tuple) {
    maxTaskPending = tuple.readParallelizationParam();
  }

  @Override
  public void nextTuple() {
    if (eCloudOutputCollector.getPendingCount() < maxTaskPending) {
      super.nextTuple();
    }
  }

}
