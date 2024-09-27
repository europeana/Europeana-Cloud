package eu.europeana.cloud.service.dps.services.submitters;

import static eu.europeana.cloud.service.dps.storm.utils.TopologiesNames.DEPUBLICATION_TOPOLOGY;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import eu.europeana.indexing.Indexer;
import java.util.Arrays;
import java.util.Date;
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
      IndexWrapper indexWrapper) {
    this.filesCounterFactory = filesCounterFactory;
    this.taskStatusUpdater = taskStatusUpdater;
    this.kafkaTopicSelector = kafkaTopicSelector;
    this.recordSubmitService = recordSubmitService;
    this.indexWrapper = indexWrapper;
  }

  @Override
  public void submitTask(SubmitTaskParameters parameters) throws TaskSubmissionException {
    int expectedSize = evaluateTaskSize(parameters);
    LOGGER.info("The task {} is in a pending mode.Expected size: {}", parameters.getTask().getTaskId(), expectedSize);

    if (expectedSize == 0) {
      taskStatusUpdater.setTaskDropped(parameters.getTask().getTaskId(), "The task doesn't include any records");
      return;
    }

    selectKafkaQueue(parameters);

    LOGGER.debug("Sending task id={} to topology {} by kafka topic {}. Parameters:\n{}",
        parameters.getTask().getTaskId(), parameters.getTaskInfo().getTopologyName(), parameters.getTopicName(), parameters);

    Stream<String> recordsForDepublication = fetchRecordIdentifiers(parameters);
    submitRecords(recordsForDepublication, parameters);
  }

  private void submitRecords(Stream<String> recordsForDepublication, SubmitTaskParameters parameters) {
    recordsForDepublication.forEach(recordId -> {
      DpsRecord aRecord = DpsRecord.builder()
                                   .taskId(parameters.getTask().getTaskId())
                                   .recordId(recordId)
                                   .build();
      recordSubmitService.submitRecord(aRecord, parameters);
    });
  }

  private void selectKafkaQueue(SubmitTaskParameters parameters) {
    String preferredTopicName = kafkaTopicSelector.findPreferredTopicNameFor(parameters.getTaskInfo().getTopologyName());
    parameters.setTopicName(preferredTopicName);
    taskStatusUpdater.updateSubmitParameters(parameters);
  }

  private Stream<String> fetchRecordIdentifiers(SubmitTaskParameters parameters) throws TaskSubmissionException {
    if (isRecordsDepublication(parameters)) {
      return Arrays.stream(
          parameters.getTask().getParameter(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH).split(","));
    } else if (isDatasetDepublication(parameters)) {
      Indexer indexer = indexWrapper.getIndexer(TargetIndexingDatabase.PREVIEW);
      return indexer.getRecordIds(parameters.getTaskParameter(PluginParameterKeys.METIS_DATASET_ID),
          new Date());
    } else {
      throw new TaskSubmissionException("Unknown depublication task type");
    }
  }

  private int evaluateTaskSize(SubmitTaskParameters parameters) throws TaskSubmissionException {
    LOGGER.info("Evaluating size of depublication task for task_id {} ", parameters.getTask().getTaskId());
    DpsTask task = parameters.getTask();
    int expectedSize = filesCounterFactory.createFilesCounter(task, DEPUBLICATION_TOPOLOGY).getFilesCount(task);
    parameters.getTaskInfo().setExpectedRecordsNumber(expectedSize);
    LOGGER.info("Evaluated size: {} for task id {}", expectedSize, task.getTaskId());
    taskStatusUpdater.updateStatusExpectedSize(task.getTaskId(), TaskState.DEPUBLISHING, expectedSize);
    return expectedSize;
  }

  private boolean isRecordsDepublication(SubmitTaskParameters parameters) {
    return parameters.getTask().getParameters().containsKey(PluginParameterKeys.RECORD_IDS_TO_DEPUBLISH);
  }

  private boolean isDatasetDepublication(SubmitTaskParameters parameters) {
    return parameters.getTask().getParameters().containsKey(PluginParameterKeys.METIS_DATASET_ID);
  }
}
