package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.service.dps.DepublicationInfo;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.storm.utils.ExpectedCounters;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskDroppedException;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.utils.KafkaTopicSelector;
import eu.europeana.cloud.service.dps.utils.files.counter.FilesCounterFactory;
import eu.europeana.corelib.solr.bean.impl.FullBeanImpl;
import eu.europeana.indexing.Indexer;
import eu.europeana.indexing.exception.IndexingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static eu.europeana.cloud.service.dps.storm.utils.TopologiesNames.DEPUBLICATION_TOPOLOGY;

/**
 * Class responsible for submitting (sending to Kafka queue) records that should be depublished
 */
@Service
public class DepublicationTaskSubmitter implements TaskSubmitter {

  private final IndexWrapper indexWrapper;
  private final RecordSubmitService recordSubmitService;
  private final TaskStatusChecker taskStatusChecker;

  /**
   * Default constructor
   *
   * @param recordSubmitService service needed for sending records for queue
   * @param indexWrapper        service needed for accessing indexer
   */
  public DepublicationTaskSubmitter(RecordSubmitService recordSubmitService, IndexWrapper indexWrapper,
      TaskStatusChecker taskStatusChecker) {
    this.recordSubmitService = recordSubmitService;
    this.indexWrapper = indexWrapper;
    this.taskStatusChecker = taskStatusChecker;
  }

  @Override
  public ExpectedCounters submitTask(SubmitTaskParameters parameters) throws TaskSubmissionException, TaskDroppedException {
    Stream<String> recordsForDepublication = fetchRecordIdentifiers(parameters);
    int sentRecordCount = submitRecords(recordsForDepublication, parameters);
    return new ExpectedCounters(sentRecordCount,0);
  }


  private int submitRecords(Stream<String> recordsForDepublication, SubmitTaskParameters parameters) throws TaskDroppedException {
    long taskId = parameters.getTask().getTaskId();
    int recordCounter = 0;
    Iterable<String> oneTimeIterable = recordsForDepublication::iterator;
    for(String recordId: oneTimeIterable){
      checkIfTaskIsKilled(parameters.getTask());
      DpsRecord aRecord = DpsRecord.builder()
                                   .taskId(taskId)
                                   .recordId(recordId)
                                   .build();
      if (recordSubmitService.submitRecord(aRecord, parameters)) {
        recordCounter++;
      }
    }
    return recordCounter;
  }

  private Stream<String> fetchRecordIdentifiers(SubmitTaskParameters parameters) throws TaskSubmissionException {
    try {
      DepublicationInfo info = (DepublicationInfo) parameters.getTask().getSource();
      if (info.isDepublishWholeDataset()) {
        Indexer<FullBeanImpl> indexer = indexWrapper.getIndexer(TargetIndexingDatabase.PUBLISH);
        return indexer.getRecordIds(parameters.getTaskParameter(PluginParameterKeys.METIS_DATASET_ID), new Date(), 5000);
      } else {
        return info.getEuropeanaIdsToDepublish().stream();
      }
    } catch (IndexingException e) {
      throw new TaskSubmissionException("Fetching record identifiers failed", e);
    }
  }

  private void checkIfTaskIsKilled(DpsTask task) throws TaskDroppedException {
    if (taskStatusChecker.hasDroppedStatus(task.getTaskId())) {
      throw new TaskDroppedException(task);
    }
  }
}
