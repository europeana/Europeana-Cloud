package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.oaipmh.Harvester;
import eu.europeana.cloud.service.dps.oaipmh.Harvester.CancelTrigger;
import eu.europeana.cloud.service.dps.oaipmh.HarvesterException;
import eu.europeana.cloud.service.dps.oaipmh.HarvesterFactory;
import eu.europeana.cloud.service.dps.storm.utils.CassandraSubTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

/**
 * Class wrapped eu.europeana.cloud.service.dps.oaipmh.Harvester implementation
 * Prepare parameters for given topologyName (java.lang.String) and  task (eu.europeana.cloud.service.dps.DpsTask)
 * Harvested records are submitted to Kafka service
 */
public class HarvesterWrapper {
    /** Default logger for class */
    private static final Logger LOGGER = LoggerFactory.getLogger(HarvesterWrapper.class);

    /** Number of retries when http request fails */
    private static final int DEFAULT_RETRIES = 3;

    /** Wait time between two http request retries */
    private static final int SLEEP_TIME = 5000;

    /** Kafka service to store {@link eu.europeana.cloud.service.dps.DpsRecord} items */
    @Autowired
    private RecordExecutionSubmitService recordSubmitService;

    /** Auxiliary object to check 'kill flag' for task */
    @Autowired
    private TaskStatusChecker taskStatusChecker;

    /** DAO to store record info as subTask in notification table */
    @Autowired
    private CassandraSubTaskInfoDAO subTaskInfoDAO;

    /**
     * Read/check harvesting parameters from task and starts harvesting data for given task
     * @param topologyName Name of topology
     * @param task Processed task
     * @return Number of real harvested records
     * @throws HarvesterException Throws if harvesting goes wrong
     */
    public int harvest(String topologyName, DpsTask task) throws HarvesterException {
        Harvester harvester = HarvesterFactory.createHarvester(DEFAULT_RETRIES, SLEEP_TIME);

        OAIPMHHarvestingDetails oaipmhHarvestingDetails =
                task.getHarvestingDetails() != null ?
                        task.getHarvestingDetails() : new OAIPMHHarvestingDetails();

        String oaiEndPoint = task.getDataEntry(InputDataType.REPOSITORY_URLS).get(0);

        Set<String> metadataPrefixes = getMetadataPrefixes(harvester, oaiEndPoint, oaipmhHarvestingDetails);
        Set<String> sets = oaipmhHarvestingDetails.getSets();

        int harvestedCount = 0;

        for (String metadataPrefixe : metadataPrefixes) {
            if(sets == null || sets.isEmpty()) {
                harvestedCount += harvestIdentifiers(
                        harvester,
                        topologyName,
                        task.getTaskId(),
                        oaiEndPoint,
                        metadataPrefixe,
                        null,
                        oaipmhHarvestingDetails.getDateFrom(),
                        oaipmhHarvestingDetails.getDateUntil(),
                        oaipmhHarvestingDetails.getExcludedSets(),
                        harvestedCount
                );
            } else {
                for(String dataset: sets) {
                    harvestedCount += harvestIdentifiers(
                            harvester,
                            topologyName,
                            task.getTaskId(),
                            oaiEndPoint,
                            metadataPrefixe,
                            dataset,
                            oaipmhHarvestingDetails.getDateFrom(),
                            oaipmhHarvestingDetails.getDateUntil(),
                            oaipmhHarvestingDetails.getExcludedSets(),
                            harvestedCount
                    );
                }
            }
        }
        return harvestedCount;
    }

    /**
     * Harvest identifiers using prepared parameters and store {@link DpsRecord to Kafka service }
     * Method put trigger to Harvester which check 'kill flag' in task
     * @param harvester
     * @param topologyName Topology name
     * @param taskId Task identifier
     * @param oaiPmhEndpoint URL to harvested place in Internet
     * @param metadataPrefix Metadata prefix
     * @param dataset Dataset name (can be <code>null</code>)
     * @param fromDate Beginning date of harvested records
     * @param untilDate Ending date of harvested records
     * @param excludedSets Set of records should not be harvested
     * @param resourceBaseNumber Base number for inserting record as subtask to db. Firs harvested recorde get
     *                           resourceBaseNumber+1, second on resourceBaseNumber+2 and so on
     * @return Number of real harvested records (and stored to Kafka service)
     * @throws HarvesterException Throws if harvesting goes wrong
     */
    private int harvestIdentifiers(Harvester harvester, String topologyName, final long taskId, String oaiPmhEndpoint,
                                   String metadataPrefix, String dataset, Date fromDate, Date untilDate,
                                   Set<String> excludedSets, int resourceBaseNumber) throws HarvesterException {
        final CancelTrigger cancelTrigger = new CancelTrigger() {
            @Override
            public boolean shouldCancel() {
                return taskStatusChecker.hasKillFlag(taskId);
            }
        };
        final List<String> identifiers =
            harvester.harvestIdentifiers(metadataPrefix, dataset, fromDate, untilDate, oaiPmhEndpoint, excludedSets, cancelTrigger);

        for (String identifier : identifiers){
            resourceBaseNumber++;
            recordSubmitService.submitRecord(new DpsRecord(taskId, identifier, metadataPrefix), topologyName);
            subTaskInfoDAO.insert(resourceBaseNumber, taskId, topologyName, identifier, States.QUEUED.toString(), "", "", "");
        }
        return identifiers.size();
    }

    /**
     * Auxiliary method to resolve the metadata prefixes.
     * If metedata prefixes list in oaipmhHarvestingDetails is empty (or null), method read metadata prefixes from given URL (oaiPmhEndpoint)
     * Otherwise given metadata prefixes is used. Then from this list excluded prefixes elements (stored in oaipmhHarvestingDetails) are removed
     * @param oaiPmhEndpoint URL to harvested place in Internet
     * @param oaipmhHarvestingDetails Set of harvesting parameters
     * @return List of prefixes (all from oaipmhHarvestingDetails or all read from oaiPmhEndpoint) except elements on excluded list
     * @throws HarvesterException Throws if reading metadata prefixes from oaiPmhEndpoint goes wrong
     */
    private Set<String> getMetadataPrefixes(Harvester harvester, String oaiPmhEndpoint, OAIPMHHarvestingDetails oaipmhHarvestingDetails) throws HarvesterException {
        Set<String> result = new HashSet<>();

        if(oaipmhHarvestingDetails.getSchemas() == null || oaipmhHarvestingDetails.getSchemas().isEmpty()) {
            //All metadata prefixes
            final Set<String> excludedMetadataPrefixes = oaipmhHarvestingDetails.getExcludedSchemas();;
            result = harvester.getSchemas(oaiPmhEndpoint, excludedMetadataPrefixes);
        } else {
            //Specific metadata prefixes
            result.addAll(oaipmhHarvestingDetails.getSchemas());
            Set<String> excludedMetadataPrefixes = oaipmhHarvestingDetails.getExcludedSchemas();
            if (excludedMetadataPrefixes != null) {
                result.removeAll(excludedMetadataPrefixes);
            }
        }
        return result;
    }
}
