package eu.europeana.cloud.service.dps.services.postprocessors;

import static eu.europeana.cloud.service.dps.Constants.DPS_REPRESENTATION_NAME;

import com.google.common.collect.Iterators;
import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.dps.EngineTaskState;
import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.commons.urls.RepresentationParser;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;
import eu.europeana.cloud.service.dps.BatchInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.metis.indexing.TargetIndexingDatabase;
import eu.europeana.cloud.service.dps.service.utils.indexing.IndexWrapper;
import eu.europeana.cloud.service.dps.storm.dao.ExistingInMetisHarvestedRecordsBatchCompleter;
import eu.europeana.cloud.service.dps.storm.dao.HarvestedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.dao.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.HarvestedRecord;
import eu.europeana.cloud.service.dps.storm.utils.TaskDroppedException;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusUpdater;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.indexing.exception.IndexingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;

/**
 * Service responsible for executing postprocessing for the OAI and HTTP tasks. It will be done in the following way: <br/>
 *
 * <ol>
 *  <li>Synchronize the table <b>harvested_records</b> with information from Metis databases both the PREVIEW and PUBLISH.
 *  Fill columns:
 *     <ul>
 *         <li>preview_harvest_date</li>
 *         <li>preview_harvest_md5</li>
 *         <li>published_harvest_date</li>
 *         <li>published_harvest_md5</li>
 *    </ul>
 *  for records that are in Metis, but have these columns empty or have no row in <b>harvested_records</b> table at all.
 *  <br>This is a fuse for cases previous indexing executions were not properly finished, or the <b>harvested_records</b>
 *  table, is not up to date with the state of the Metis database for any reason. This makes the full workflow processing
 *  (and partially also incremental processing) self-healing.<br>More info in:
 *  {@link eu.europeana.cloud.service.dps.storm.dao.ExistingInMetisHarvestedRecordsBatchCompleter}.
 *  <li>Iterate over oll records from table <b>harvested_records</b> where <b>latest_harvest_date</b> < <b>task_execution_date</b></li>
 *  <li>For each europeana_id:</li>
 *      <ul>
 *          <li>find cloud_id</li>
 *          <li>create representation version</li>
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

  private final UISClient uisClient;
  private final IndexWrapper indexWrapper;

  @SuppressWarnings("java:S107") //We want to use constructor injection so we need 8 parameters here
  public HarvestingPostProcessor(HarvestedRecordsDAO harvestedRecordsDAO,
      ProcessedRecordsDAO processedRecordsDAO,
      RecordServiceClient recordServiceClient,
      UISClient uisClient,
      TaskStatusUpdater taskStatusUpdater,
      TaskStatusChecker taskStatusChecker,
      IndexWrapper indexWrapper) {
    super(taskStatusChecker, taskStatusUpdater, harvestedRecordsDAO);
    this.processedRecordsDAO = processedRecordsDAO;
    this.recordServiceClient = recordServiceClient;
    this.uisClient = uisClient;
    this.indexWrapper = indexWrapper;
  }

  @Override
  public void executePostprocessing(TaskInfo taskInfo, DpsTask dpsTask) {
    try {
      taskStatusUpdater.updateState(dpsTask.getTaskId(), EngineTaskState.IN_POST_PROCESSING,
          "Postprocessing - synchronizing existing records from Metis.");
      updateHarvestedRecordsTableWithRecordsExistingInMetis(dpsTask);
      taskStatusUpdater.updateExpectedDepublishAndPostprocessedRecordsNumber(dpsTask.getTaskId(),
          Iterators.size(fetchDeletedRecords(dpsTask)));
      taskStatusUpdater.updateState(dpsTask.getTaskId(), EngineTaskState.IN_POST_PROCESSING,
          "Postprocessing - adding removed records to result batch.");
      addDeletedRecordsToTaskResultBatch(dpsTask);
      taskStatusUpdater.setTaskCompletelyProcessed(dpsTask.getTaskId(), "PROCESSED");
    } catch (RetryInterruptedException e) {
      throw e;
    } catch (Exception exception) {
      throw new PostProcessingException(
          String.format("Error while %s post-process given task: taskId=%d. Cause: %s", getClass().getSimpleName(),
              dpsTask.getTaskId(), exception.getMessage() != null ? exception.getMessage() : exception.toString()), exception);
    }
  }

  private void addDeletedRecordsToTaskResultBatch(DpsTask dpsTask) throws TaskDroppedException {
    Iterator<HarvestedRecord> it = fetchDeletedRecords(dpsTask);
    int postProcessedRecordsCount = 0;
    int processedAndSuccessDepublishRecordCount = 0;
    while (it.hasNext()) {
      taskStatusChecker.checkNotDropped(dpsTask);
      var harvestedRecord = it.next();
      if (!isIndexedInSomeEnvironment(harvestedRecord)) {
        harvestedRecordsDAO.deleteRecord(harvestedRecord.getMetisDatasetId(), harvestedRecord.getRecordLocalId());
        LOGGER.info("Deleted: {}, cause it is not present in source and also it is not indexed in any environment, taskId={}"
            , harvestedRecord, dpsTask.getTaskId());
        postProcessedRecordsCount++;
      } else if (!isRecordProcessed(dpsTask, harvestedRecord)) {
        createPostProcessedRecord(dpsTask, harvestedRecord);
        markHarvestedRecordAsProcessed(dpsTask, harvestedRecord);
        postProcessedRecordsCount++;
        processedAndSuccessDepublishRecordCount++;
        taskStatusUpdater.updatePostProcessingAndDepublishCounters(dpsTask.getTaskId(), postProcessedRecordsCount, processedAndSuccessDepublishRecordCount);
        LOGGER.info("Added deleted record {} to batch, taskId={}", harvestedRecord, dpsTask.getTaskId());
      } else {
        LOGGER.info("Omitted record {} cause it was already added to batch, taskId={}", harvestedRecord,
            dpsTask.getTaskId());
      }

    }
  }

  private void updateHarvestedRecordsTableWithRecordsExistingInMetis(DpsTask task) throws IndexingException {
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
      String cloudId = findOrCreateCloudId(dpsTask, harvestedRecord);
      createRepresentationVersion(dpsTask, cloudId);
      LOGGER.info("Created representation of deleted record found in postprocessing for: {}", harvestedRecord);
    } catch (CloudException | MCSException | MalformedURLException e) {
      throw new PostProcessingException("Could not add deleted record id=" + harvestedRecord.getRecordLocalId()
          + " to task result batch! taskId=" + dpsTask.getTaskId(), e);
    }
  }

  private Iterator<HarvestedRecord> fetchDeletedRecords(DpsTask task) {
    var harvestDate = DateHelper.parseISODate(task.getParameter(PluginParameterKeys.HARVEST_DATE));
    Iterator<HarvestedRecord> allRecords =
        harvestedRecordsDAO.findDatasetRecords(task.getParameter(PluginParameterKeys.METIS_DATASET_ID));
    return Iterators.filter(allRecords, theRecord ->
        latestHarvestDateNotFromCurrentHarvest(theRecord, harvestDate));
  }

  private boolean latestHarvestDateNotFromCurrentHarvest(HarvestedRecord theRecord, Date harvestDate) {
    return (theRecord.getLatestHarvestDate() == null) || theRecord.getLatestHarvestDate().before(harvestDate);
  }

  private String findOrCreateCloudId(DpsTask dpsTask, HarvestedRecord harvestedRecord) throws CloudException {
    String providerId = dpsTask.getResultsBatch().getProviderId();
    //We support the situation when the records are not in the eCloud at all, so we need to create cloudId if it does not exist.
    return uisClient.createCloudId(providerId, harvestedRecord.getRecordLocalId()).getId();
  }

  private Representation createRepresentationVersion(DpsTask dpsTask, String cloudId) throws MCSException, MalformedURLException {
    BatchInfo output = dpsTask.getResultsBatch();
    var representationUri = recordServiceClient.createRepresentation(cloudId, DPS_REPRESENTATION_NAME, output.getProviderId(),
        null, output.getBatchId(), true);
    return RepresentationParser.parseResultUrl(
        representationUri);
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
