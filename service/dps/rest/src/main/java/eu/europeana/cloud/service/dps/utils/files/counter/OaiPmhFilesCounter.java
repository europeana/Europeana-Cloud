package eu.europeana.cloud.service.dps.utils.files.counter;

import static eu.europeana.cloud.common.model.dps.TaskInfo.UNKNOWN_EXPECTED_RECORDS_NUMBER;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.utils.DpsTaskToOaiHarvestConverter;
import eu.europeana.metis.harvesting.HarvesterException;
import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Counts the number of records to harvest from a specified OAI-PMH repository.
 */
public class OaiPmhFilesCounter extends FilesCounter {

  private static final Logger LOGGER = LoggerFactory.getLogger(OaiPmhFilesCounter.class);

  @Override
  public int getFilesCount(DpsTask task) throws TaskSubmissionException {

    LOGGER.info("Starting counting records for taskId={}", task.getTaskId());
    OaiHarvest harvestToByExecuted = new DpsTaskToOaiHarvestConverter().from(task);
    int total = 0;
    final OaiHarvester harvester = HarvesterFactory.createOaiHarvester();
    try {
      final Integer count = harvester.countRecords(harvestToByExecuted);
      if (count == null) {
        LOGGER.info(
            "Cannot count completeListSize for taskId= {}: No resumption token information found.",
            task.getTaskId());
        return UNKNOWN_EXPECTED_RECORDS_NUMBER;
      }
      total += count;
    } catch (HarvesterException e) {
      String logMessage = "Cannot complete the request for the following repository URL "
          + harvestToByExecuted.getRepositoryUrl();
      throw new TaskSubmissionException(logMessage + " Because: " + e.getMessage(), e);
    }
    LOGGER.info("Records counting finished for taskId={}. There are {} records", task.getTaskId(), total);
    return total;
  }
}
