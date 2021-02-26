package eu.europeana.cloud.service.dps.utils.files.counter;

import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.exceptions.TaskSubmissionException;
import eu.europeana.cloud.service.dps.utils.DpsTaskToOaiHarvestConverter;
import eu.europeana.metis.harvesting.HarvesterException;
import eu.europeana.metis.harvesting.HarvesterFactory;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvest;
import eu.europeana.metis.harvesting.oaipmh.OaiHarvester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Counts the number of records to harvest from a specified OAI-PMH repository.
 */
public class OaiPmhFilesCounter extends FilesCounter {

    private static final Logger LOGGER = LoggerFactory.getLogger(OaiPmhFilesCounter.class);

    @Override
    public int getFilesCount(DpsTask task) throws TaskSubmissionException {

        List<OaiHarvest> harvestsToByExecuted = new DpsTaskToOaiHarvestConverter().from(task);
        final OAIPMHHarvestingDetails harvest = task.getHarvestingDetails();
        if (taskCanBeCounted(harvest)) {
            LOGGER.info("Cannot count completeListSize for taskId= {} . Excluded sets or schemas are not supported", task.getTaskId());
            return -1;
        }
        int total = 0;
        final OaiHarvester harvester = HarvesterFactory.createOaiHarvester();
        for (OaiHarvest oaiHarvest : harvestsToByExecuted) {
            try {
                final Integer count = harvester.countRecords(oaiHarvest);
                if (count == null) {
                    LOGGER.info(
                            "Cannot count completeListSize for taskId= {}: No resumption token information found.",
                            task.getTaskId());
                    return -1;
                }
                total += count;
            } catch (HarvesterException e) {
                String logMessage = "Cannot complete the request for the following repository URL "
                        + oaiHarvest.getRepositoryUrl();
                LOGGER.info(logMessage, e);
                throw new TaskSubmissionException(logMessage + " Because: " + e.getMessage(), e);
            }
        }
        return total;
    }

    private boolean taskCanBeCounted(OAIPMHHarvestingDetails harvest){
        if (harvest.getExcludedSets() != null && !harvest.getExcludedSets().isEmpty() ||
                harvest.getExcludedSchemas() != null && !harvest.getExcludedSchemas().isEmpty()) {
            return true;
        }else
            return false;
    }
}