package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.HarvestResult;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.services.submitters.RecordSubmitService;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.HarvesterRuntimeException;
import eu.europeana.metis.harvesting.HarvestingIterator;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import eu.europeana.metis.harvesting.oaipmh.OaiRecordHeader;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class HarvestsExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(HarvestsExecutor.class);

  private static final int DEFAULT_RETRIES = 3;
  private static final int SLEEP_TIME = 5000;

  private final RecordSubmitService recordSubmitService;

  /**
   * Auxiliary object to check 'kill flag' for task
   */
  private final TaskStatusChecker taskStatusChecker;

  public HarvestsExecutor(RecordSubmitService recordSubmitService, TaskStatusChecker taskStatusChecker) {
    this.recordSubmitService = recordSubmitService;
    this.taskStatusChecker = taskStatusChecker;
  }

  public HarvestResult execute(OaiHarvest harvestToBeExecuted, SubmitTaskParameters parameters) throws HarvesterRuntimeException {

    LOGGER.info("(Re-)starting identifiers harvesting for: {}. Task identifier: {}", harvestToBeExecuted,
        parameters.getTask().getTaskId());
    OaiHarvester harvester = HarvesterFactory.createOaiHarvester(null, DEFAULT_RETRIES, SLEEP_TIME);
    HarvestingIterator<OaiRecordHeader, OaiRecordHeader> headerIterator = harvester.harvestRecordHeaders(harvestToBeExecuted);

    // *** Main harvesting loop for a given task ***
    boolean taskDropped = false;
    int resultCounter = 0;
    for (var oaiHeader : headerIterator) {
      if (taskStatusChecker.hasDroppedStatus(parameters.getTask().getTaskId())) {
        LOGGER.info("Harvesting for {} (Task: {}) stopped by external signal",
            harvestToBeExecuted, parameters.getTask().getTaskId());
        taskDropped = true;
        break;
      }

      if (oaiHeader.isDeleted()) {
        LOGGER.warn("Ignoring OAI record header {} for {} because it is deleted on the OAI repo",
            oaiHeader.getOaiIdentifier(),
            parameters.getTask().getTaskId());
        continue;
      }

      DpsRecord dpsRecord = convertToDpsRecord(oaiHeader, harvestToBeExecuted, parameters.getTask());
      if (recordSubmitService.submitRecord(dpsRecord, parameters)) {
        resultCounter++;
      }

      logProgressFor(harvestToBeExecuted, parameters.incrementAndGetPerformedRecordCounter());

      if (resultCounter >= getMaxRecordsCount(parameters)) {
        break;
      }
    }
    if (taskDropped) {
      return HarvestResult.builder()
                          .resultCounter(resultCounter)
                          .taskState(TaskState.DROPPED).build();
    }
    LOGGER.info("Identifiers harvesting finished for: {}. Counter: {}", harvestToBeExecuted, resultCounter);

    return new HarvestResult(resultCounter, TaskState.QUEUED);
  }

  private int getMaxRecordsCount(SubmitTaskParameters parameters) {
    return Optional.ofNullable(parameters.getTask().getParameter(PluginParameterKeys.SAMPLE_SIZE))  //return value SAMPLE_SIZE
                   .map(Integer::parseInt)
                   .orElse(
                       Integer.MAX_VALUE);                                                             //or MAX_VALUE if null is above
  }

  /*package visiblility*/ DpsRecord convertToDpsRecord(OaiRecordHeader oaiHeader, OaiHarvest harvest, DpsTask dpsTask) {
    return DpsRecord.builder()
                    .taskId(dpsTask.getTaskId())
                    .recordId(oaiHeader.getOaiIdentifier())
                    .metadataPrefix(harvest.getMetadataPrefix())
                    .build();
  }

  /*package visiblility*/ void logProgressFor(OaiHarvest harvest, int counter) {
    if (counter % 1000 == 0) {
      LOGGER.info("Identifiers harvesting is progressing for: {}. Current counter: {}", harvest, counter);
    }
  }
}
