package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.oaipmh.Harvester;
import eu.europeana.cloud.service.dps.oaipmh.HarvesterException;
import eu.europeana.cloud.service.dps.oaipmh.HarvesterFactory;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
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
    private ProcessedRecordsDAO processedRecordsDAO;

    /** Auxiliary object to check 'kill flag' for task */
    @Autowired
    private TaskStatusChecker taskStatusChecker;

    public void execute(String topologyName, List<Harvest> harvestsToBeExecuted, DpsTask dpsTask, String topicName) throws HarvesterException {
        int counter = 0;
        for (Harvest harvest : harvestsToBeExecuted) {
            LOGGER.info("Starting identifiers harvesting for: {}", harvest);
            Harvester harvester = HarvesterFactory.createHarvester(DEFAULT_RETRIES, SLEEP_TIME);
            Iterator<OAIHeader> headerIterator = harvester.harvestIdentifiers(harvest);

            // *** Main harvesting loop for given task ***
            while (headerIterator.hasNext() && !taskStatusChecker.hasKillFlag(dpsTask.getTaskId())) {
                OAIHeader oaiHeader = headerIterator.next();
                DpsRecord record = convertToDpsRecord(oaiHeader, harvest, dpsTask);

                sentMessage(record, topicName);
                updateRecordStatus(record, topologyName);
                logProgressFor(harvest, counter);
                counter++;
            }
            LOGGER.info("Identifiers harvesting finished for: {}. Counter: {}", harvest, counter);
        }
        if(counter == 0){
            LOGGER.info("Task dropped. No data harvested");
            taskInfoDAO.dropTask(dpsTask.getTaskId(), "The task with the submitted parameters is empty", TaskState.DROPPED.toString());
        }
    }

    private void logProgressFor(Harvest harvest, int counter) {
        if (counter % 1000 == 0) {
            LOGGER.info("Identifiers harvesting is progressing for: {}. Current counter: {}", harvest, counter);
        }
    }

    /*Merge code below when latest version of restart procedure will be done/known*/
    public void executeForRestart(String topologyName, List<Harvest> harvestsToByExecuted, DpsTask dpsTask, String topicName) throws HarvesterException {
        int counter = 0;

        for (Harvest harvest : harvestsToByExecuted) {
            LOGGER.info("(Re-)starting identifiers harvesting for: {}", harvest);
            Harvester harvester = HarvesterFactory.createHarvester(DEFAULT_RETRIES, SLEEP_TIME);
            Iterator<OAIHeader> headerIterator = harvester.harvestIdentifiers(harvest);

            // *** Main harvesting loop for given task ***
            while (headerIterator.hasNext() && !taskStatusChecker.hasKillFlag(dpsTask.getTaskId())) {
                OAIHeader oaiHeader = headerIterator.next();

                ProcessedRecord processedRecord = processedRecordsDAO.selectByPrimaryKey(dpsTask.getTaskId(), oaiHeader.getIdentifier());
                if(processedRecord == null || processedRecord.getState() == RecordState.ERROR) {
                    DpsRecord record = convertToDpsRecord(oaiHeader, harvest, dpsTask);
                    sentMessage(record, topicName);
                    updateRecordStatus(record, topologyName);
                    counter++;
                }
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

    private void updateRecordStatus(DpsRecord dpsRecord, String topologyName) {
        LOGGER.debug("Updating record in notifications table: {}", dpsRecord);
        processedRecordsDAO.insert(dpsRecord.getTaskId(), dpsRecord.getRecordId(),
                "", topologyName, RecordState.QUEUED.toString(), "", "");
    }
}
