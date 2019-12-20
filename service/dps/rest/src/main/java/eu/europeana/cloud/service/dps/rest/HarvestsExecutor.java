package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.oaipmh.Harvester;
import eu.europeana.cloud.service.dps.oaipmh.HarvesterException;
import eu.europeana.cloud.service.dps.oaipmh.HarvesterFactory;
import eu.europeana.cloud.service.dps.storm.utils.CassandraSubTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import org.dspace.xoai.serviceprovider.exceptions.BadArgumentException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Iterator;
import java.util.List;

@Service
public class HarvestsExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(HarvestsExecutor.class);

    private static final int DEFAULT_RETRIES = 3;
    private static final int SLEEP_TIME = 5000;

    @Autowired
    private RecordExecutionSubmitService recordSubmitService;

    @Autowired
    private CassandraTaskInfoDAO taskInfoDAO;

    @Autowired
    private CassandraSubTaskInfoDAO subTaskInfoDAO;

    /**
     * Auxiliary object to check 'kill flag' for task
     */
    @Autowired
    private TaskStatusChecker taskStatusChecker;

    public void execute(List<Harvest> harvestsToByExecuted, DpsTask dpsTask, String topicName) throws BadArgumentException, HarvesterException {
        int counter = 0;
        for (Harvest harvest : harvestsToByExecuted) {
            LOGGER.info("Starting identifiers harvesting for: {}", harvest);
            Harvester harvester = HarvesterFactory.createHarvester(DEFAULT_RETRIES, SLEEP_TIME);
            Iterator<OAIHeader> headerIterator = harvester.harvestIdentifiers(harvest);

            // *** Main harvesting loop for given task ***
            while (headerIterator.hasNext() && !taskStatusChecker.hasKillFlag(dpsTask.getTaskId())) {
                OAIHeader oaiHeader = headerIterator.next();
                DpsRecord record = convertToDpsRecord(oaiHeader, harvest, dpsTask);
                sentMessage(record, topicName);
                updateRecordStatus(record, ++counter);
            }
            LOGGER.info("Identifiers harvesting finished for: {}. Counter: {}", harvest, counter);
        }
        if (counter == 0) {
            LOGGER.info("Task dropped. No data harvested");
            taskInfoDAO.dropTask(dpsTask.getTaskId(), "The task with the submitted parameters is empty", TaskState.DROPPED.toString());
        }
    }

    private DpsRecord convertToDpsRecord(OAIHeader oaiHeader, Harvest harvest, DpsTask dpsTask) {
        return DpsRecord.builder()
                .taskId(dpsTask.getTaskId())
                .recordId(oaiHeader.getIdentifier())
                .metadataPrefix(harvest.getMetadataPrefix())
                .build();
    }

    private void sentMessage(DpsRecord record, String topicName) {
        LOGGER.debug("Sending records to messges queue: {}", record);
        recordSubmitService.submitRecord(record, topicName);
    }

    private void updateRecordStatus(DpsRecord dpsRecord, int counter) {
        LOGGER.debug("Updating record in notifications table: {}", dpsRecord);
        subTaskInfoDAO.insert(counter, dpsRecord.getTaskId(), "oai_topology", dpsRecord.getRecordId(), States.QUEUED.toString(), "", "", "");
    }
}
