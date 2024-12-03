package eu.europeana.cloud.service.dps.services.postprocessors;

import com.google.common.collect.Iterators;
import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.commons.urls.DataSetUrlParser;
import eu.europeana.cloud.service.commons.urls.RepresentationParser;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.storm.dao.ExistingInMetisHarvestedRecordsBatchCompleter;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.net.MalformedURLException;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service responsible for executing postprocessing for the OAI and HTTP tasks. It will be done in the following way: <br/>
 *
 * <ol>
 *  <li>Iterate over oll records from table <b>harvested_records</b> where <b>latest_harvest_date</b> < <b>task_execution_date</b></li>
 *  <li>For each europeana_id:</li>
 *      <ul>
 *          <li>find cloud_id</li>
 *          <li>create representation version</li>
 *          <li>add revision (taken from task definition (output_revision))</li>
 *          <li>add created representation version to dataset (dataset taken from task definition (output dataset))</li>
 *      </ul>
 *  <li>Change task status to PROCESSED;</li>
 * </ol>
 */
public class HarvestingPostProcessor extends TaskPostProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(HarvestingPostProcessor.class);

  private static final Set<String> PROCESSED_TOPOLOGIES =
      Set.of(TopologiesNames.OAI_TOPOLOGY, TopologiesNames.HTTP_TOPOLOGY);

  private final ProcessedRecordsDAO processedRecordsDAO;

  private final RecordServiceClient recordServiceClient;

  private final RevisionServiceClient revisionServiceClient;

  private final UISClient uisClient;
  private final IndexWrapper indexWrapper;

  public HarvestingPostProcessor(HarvestedRecordsDAO harvestedRecordsDAO,
      ProcessedRecordsDAO processedRecordsDAO,
      RecordServiceClient recordServiceClient,
      RevisionServiceClient revisionServiceClient,
      UISClient uisClient,
      TaskStatusUpdater taskStatusUpdater,
      TaskStatusChecker taskStatusChecker,
      IndexWrapper indexWrapper) {
    super(taskStatusChecker, taskStatusUpdater, harvestedRecordsDAO);
    this.processedRecordsDAO = processedRecordsDAO;
    this.recordServiceClient = recordServiceClient;
    this.revisionServiceClient = revisionServiceClient;
    this.uisClient = uisClient;
    this.indexWrapper = indexWrapper;
  }

  @Override
  public void executePostprocessing(TaskInfo taskInfo, DpsTask dpsTask) {
    try {
      taskStatusUpdater.updateState(dpsTask.getTaskId(), TaskState.IN_POST_PROCESSING,
          "Postprocessing - synchronizing existing records from Metis.");
      updateHarvestedRecordsTableWithRecordsExistingInMetis(dpsTask);
      taskStatusUpdater.updateExpectedPostProcessedRecordsNumber(dpsTask.getTaskId(),
          Iterators.size(fetchDeletedRecords(dpsTask)));
      taskStatusUpdater.updateState(dpsTask.getTaskId(), TaskState.IN_POST_PROCESSING,
          "Postprocessing - adding removed records to result revision.");
      if (addDeletedRecordsToTaskResultRevision(dpsTask)) {
        return;
      }
      taskStatusUpdater.setTaskCompletelyProcessed(dpsTask.getTaskId(), "PROCESSED");
    } catch (RetryInterruptedException e) {
      throw e;
    } catch (Exception exception) {
      throw new PostProcessingException(
          String.format("Error while %s post-process given task: taskId=%d. Cause: %s", getClass().getSimpleName(),
              dpsTask.getTaskId(), exception.getMessage() != null ? exception.getMessage() : exception.toString()), exception);
    }
  }

  private boolean addDeletedRecordsToTaskResultRevision(DpsTask dpsTask) {
    Iterator<HarvestedRecord> it = fetchDeletedRecords(dpsTask);
    int postProcessedRecordsCount = 0;
    while (it.hasNext()) {
      taskStatusChecker.checkNotDropped(dpsTask);
      var harvestedRecord = it.next();
      if (!isIndexedInSomeEnvironment(harvestedRecord)) {
        harvestedRecordsDAO.deleteRecord(harvestedRecord.getMetisDatasetId(), harvestedRecord.getRecordLocalId());
        LOGGER.info("Deleted: {}, cause it is not present in source and also it is not indexed in any environment, taskId={}"
            , harvestedRecord, dpsTask.getTaskId());
      } else if (!isRecordProcessed(dpsTask, harvestedRecord)) {
        createPostProcessedRecord(dpsTask, harvestedRecord);
        markHarvestedRecordAsProcessed(dpsTask, harvestedRecord);
        postProcessedRecordsCount++;
        taskStatusUpdater.updatePostProcessedRecordsCount(dpsTask.getTaskId(), postProcessedRecordsCount);
        LOGGER.info("Added deleted record {} to revision, taskId={}", harvestedRecord, dpsTask.getTaskId());
      } else {
        LOGGER.info("Omitted record {} cause it was already added to revision, taskId={}", harvestedRecord,
            dpsTask.getTaskId());
      }

    }
    return false;
  }

  private void updateHarvestedRecordsTableWithRecordsExistingInMetis(DpsTask task) {
    String metisDatasetId = task.getParameter(PluginParameterKeys.METIS_DATASET_ID);
    for(TargetIndexingDatabase db:TargetIndexingDatabase.values()) {
      try (ExistingInMetisHarvestedRecordsBatchCompleter completer
          = new ExistingInMetisHarvestedRecordsBatchCompleter(harvestedRecordsDAO, metisDatasetId, db)) {
        indexWrapper.getIndexer(db).getRecordIds(metisDatasetId, new Date()).forEach(completer::executeRecord);
      }
    }
  }

  public Set<String> getProcessedTopologies() {
    return PROCESSED_TOPOLOGIES;
  }

  private boolean isIndexedInSomeEnvironment(HarvestedRecord harvestedRecord) {
    return (harvestedRecord.getPreviewHarvestDate() != null) || (harvestedRecord.getPublishedHarvestDate() != null);
  }

  private void createPostProcessedRecord(DpsTask dpsTask, HarvestedRecord harvestedRecord) {
    try {
      LOGGER.info("Creating representation of deleted record found in postprocessing for: {}", harvestedRecord);
      String cloudId = findCloudId(dpsTask, harvestedRecord);
      var representation = createRepresentationVersion(dpsTask, cloudId);
      addRevisionToRepresentation(dpsTask, representation);
    } catch (CloudException | MCSException | MalformedURLException e) {
      throw new PostProcessingException("Could not add deleted record id=" + harvestedRecord.getRecordLocalId()
          + " to task result revision! taskId=" + dpsTask.getTaskId(), e);
    }
  }

  private Iterator<HarvestedRecord> fetchDeletedRecords(DpsTask task) {
    var harvestDate = DateHelper.parseISODate(task.getParameter(PluginParameterKeys.HARVEST_DATE));
    Iterator<HarvestedRecord> allRecords =
        harvestedRecordsDAO.findDatasetRecords(task.getParameter(PluginParameterKeys.METIS_DATASET_ID));
    return Iterators.filter(allRecords, theRecord -> theRecord.getLatestHarvestDate().before(harvestDate));
  }

  private String findCloudId(DpsTask dpsTask, HarvestedRecord harvestedRecord) throws CloudException {
    String providerId = dpsTask.getParameter(PluginParameterKeys.PROVIDER_ID);
    return uisClient.getCloudId(providerId, harvestedRecord.getRecordLocalId()).getId();
  }

  private Representation createRepresentationVersion(DpsTask dpsTask, String cloudId) throws MCSException, MalformedURLException {
    String providerId = dpsTask.getParameter(PluginParameterKeys.PROVIDER_ID);
    String representationName = dpsTask.getParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME);
    var datasetId = DataSetUrlParser.parse(dpsTask.getParameter(PluginParameterKeys.OUTPUT_DATA_SETS)).getId();
    var representationUri = recordServiceClient.createRepresentation(cloudId, representationName, providerId,
        datasetId);
    return RepresentationParser.parseResultUrl(representationUri);
  }

  private void addRevisionToRepresentation(DpsTask dpsTask, Representation representation) throws MCSException {
    var revision = new Revision(dpsTask.getOutputRevision());
    revision.setDeleted(true);
    revisionServiceClient.addRevision(representation.getCloudId(), representation.getRepresentationName(),
        representation.getVersion(), revision);
  }

  private void markHarvestedRecordAsProcessed(DpsTask dpsTask, HarvestedRecord harvestedRecord) {
    processedRecordsDAO.insert(ProcessedRecord.builder().taskId(dpsTask.getTaskId())
                                              .recordId(harvestedRecord.getRecordLocalId()).state(RecordState.SUCCESS)
                                              .starTime(new Date()).build());
  }

  private boolean isRecordProcessed(DpsTask dpsTask, HarvestedRecord harvestedRecord) {
    return processedRecordsDAO.selectByPrimaryKey(dpsTask.getTaskId(), harvestedRecord.getRecordLocalId())
                              .map(theRecord -> theRecord.getState() == RecordState.SUCCESS)
                              .orElse(false);
  }

  public boolean needsPostProcessing(DpsTask task) {
    return true;
  }

}
