package eu.europeana.cloud.service.dps.rest;

import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.*;
import eu.europeana.cloud.service.dps.storm.utils.CassandraSubTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
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

    @Autowired
    private RecordExecutionSubmitService recordSubmitService;

    @Autowired
    private CassandraTaskInfoDAO taskInfoDAO;

    @Autowired
    private CassandraSubTaskInfoDAO subTaskInfoDAO;

    public void execute(List<Harvest> harvestsToByExecuted, DpsTask dpsTask) throws BadArgumentException {
        int counter = 0;
        for (Harvest harvest : harvestsToByExecuted) {
            LOGGER.info("Starting identifiers harvesting for: {}", harvest);
            IdentifiersHarvester identifiersHarvester = new IdentifiersHarvester(harvest);
            Iterator<OAIHeader> headerIterator = identifiersHarvester.harvestIdentifiers();
            while (headerIterator.hasNext()) {
                OAIHeader oaiHeader = headerIterator.next();
                DpsRecord record = convertToDpsRecord(oaiHeader, harvest, dpsTask);
                sentMessage(record);
                updateRecordStatus(record, ++counter);
            }
            LOGGER.info("Identifiers harvesting finished for: {}", harvest);
        }
        if(counter == 0){
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

    private void sentMessage(DpsRecord record) {
        LOGGER.debug("Sending records to messges queue: {}", record);
        recordSubmitService.submitRecord(record, "oai_topology");
    }

    private void updateRecordStatus(DpsRecord dpsRecord, int counter) {
        LOGGER.debug("Updating record in notifications table: {}", dpsRecord);
        subTaskInfoDAO.insert(counter, dpsRecord.getTaskId(), "oai_topology", dpsRecord.getRecordId(), States.QUEUED.toString(), "", "", "");
    }
}
