package eu.europeana.cloud.service.dps.services.submitters;

import static eu.europeana.cloud.common.model.dps.TaskInfo.UNKNOWN_EXPECTED_RECORDS_NUMBER;
import static eu.europeana.cloud.service.dps.storm.utils.TopologiesNames.DEPUBLICATION_TOPOLOGY;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.utils.TaskDroppedException;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import eu.europeana.indexing.Indexer;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Class responsible for submitting (sending to Kafka queue) records that should be depublished
 */
@Service
public class DepublicationTaskSubmitter implements TaskSubmitter {

  private static final Logger LOGGER = LoggerFactory.getLogger(DepublicationTaskSubmitter.class);

  private final FilesCounterFactory filesCounterFactory;
  private final TaskStatusUpdater taskStatusUpdater;
  private final IndexWrapper indexWrapper;
  private final KafkaTopicSelector kafkaTopicSelector;
  private final RecordSubmitService recordSubmitService;
  private final TaskStatusChecker taskStatusChecker;

  /**
   * Default constructor
   *
   * @param filesCounterFactory factory for records counters
   * @param taskStatusUpdater service needed for changing task status during submission
   * @param kafkaTopicSelector service needed for selecting kafka queue for task
   * @param recordSubmitService service needed for sending records for queue
   * @param indexWrapper service needed for accessing indexer
   */
  public DepublicationTaskSubmitter(FilesCounterFactory filesCounterFactory,
      TaskStatusUpdater taskStatusUpdater, KafkaTopicSelector kafkaTopicSelector, RecordSubmitService recordSubmitService,
      IndexWrapper indexWrapper, TaskStatusChecker taskStatusChecker) {
    this.filesCounterFactory = filesCounterFactory;
    this.taskStatusUpdater = taskStatusUpdater;
    this.kafkaTopicSelector = kafkaTopicSelector;
    this.recordSubmitService = recordSubmitService;
    this.indexWrapper = indexWrapper;
    this.taskStatusChecker = taskStatusChecker;
  }

  @Override
  public void submitTask(SubmitTaskParameters parameters) throws TaskSubmissionException {
    long taskId = parameters.getTask().getTaskId();
    if (parameters.getTaskInfo().getExpectedRecordsNumber() == UNKNOWN_EXPECTED_RECORDS_NUMBER) {
      int expectedCount = evaluateTaskSize(parameters);
      if (expectedCount == 0) {
        taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(), "The task doesn't include any records");
        return;
      }

    } else {
      LOGGER.info("The task: {} already have estimated expected size: {}",
          taskId, parameters.getTaskInfo().getExpectedRecordsNumber());
      //This means that the task was restarted, we could not evaluate size again because some of the records,
      // coudl be already depublished and therefore not present in the Metis, so the size would be estimated smaller.
    }

    selectKafkaQueue(parameters);
    taskStatusUpdater.updateSubmitParameters(parameters);

    LOGGER.info("Sending task id={} to topology {} by kafka topic {}. Parameters:\n{}",
        taskId, parameters.getTaskInfo().getTopologyName(), parameters.getTopicName(), parameters);

    Stream<String> recordsForDepublication = fetchRecordIdentifiers(parameters);
    int sentRecordCount = submitRecords(recordsForDepublication, parameters);

    taskStatusUpdater.updateState(taskId, TaskState.QUEUED);
    LOGGER.info("Submitting {} records of task id={} to Kafka succeeded.", sentRecordCount, taskId);
  }

  private int submitRecords(Stream<String> recordsForDepublication, SubmitTaskParameters parameters) {
    long taskId = parameters.getTask().getTaskId();
    AtomicInteger recordCounter = new AtomicInteger(0);
    recordsForDepublication.forEach(recordId -> {
      checkIfTaskIsKilled(parameters.getTask());
      DpsRecord aRecord = DpsRecord.builder()
                                   .taskId(taskId)
                                   .recordId(recordId)
                                   .build();
      if (recordSubmitService.submitRecord(aRecord, parameters)) {
        recordCounter.incrementAndGet();
      }
    });
    return recordCounter.get();
  }

  private void selectKafkaQueue(SubmitTaskParameters parameters) {
    String preferredTopicName = kafkaTopicSelector.findPreferredTopicNameFor(parameters.getTaskInfo().getTopologyName());
    parameters.setTopicName(preferredTopicName);
  }

  private Stream<String> fetchRecordIdentifiers(SubmitTaskParameters parameters) {
    if (isRecordsDepublication(parameters)) {
      return Arrays.stream(
          parameters.getTask().getParameter(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH).split(","));
    } else {
      Indexer indexer = indexWrapper.getIndexer(TargetIndexingDatabase.PUBLISH);
      return indexer.getRecordIds(parameters.getTaskParameter(PluginParameterKeys.METIS_DATASET_ID),
          new Date());
    }
  }

  private int evaluateTaskSize(SubmitTaskParameters parameters) throws TaskSubmissionException {
    LOGGER.info("Evaluating size of depublication task for task_id {} ", parameters.getTask().getTaskId());
    DpsTask task = parameters.getTask();
    int expectedSize = filesCounterFactory.createFilesCounter(task, DEPUBLICATION_TOPOLOGY).getFilesCount(task);
    parameters.getTaskInfo().setExpectedRecordsNumber(expectedSize);
    LOGGER.info("Evaluated size: {} for task id: {}", expectedSize, task.getTaskId());
    return expectedSize;
  }

  private boolean isRecordsDepublication(SubmitTaskParameters parameters) {
    return parameters.getTask().getParameters().containsKey(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH);
  }

  private void checkIfTaskIsKilled(DpsTask task) {
    if (taskStatusChecker.hasDroppedStatus(task.getTaskId())) {
      throw new TaskDroppedException(task);
    }
  }
}
