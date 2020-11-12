package eu.europeana.cloud.service.dps.services.submitters;

import eu.europeana.cloud.common.model.dps.ProcessedRecord;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.RecordExecutionSubmitService;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import eu.europeana.cloud.service.dps.storm.utils.ProcessedRecordsDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Optional;

/**
 * Submits record to storm cluster by sending it to valid Kafka topic. Class is also responsible for adding suitable
 * entry in processedRecordState table, and based on this table not sending record twice, in case of retry or
 * duplication detection.
 */
public class RecordSubmitService {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordSubmitService.class);

    private final ProcessedRecordsDAO processedRecordsDAO;

    private final RecordExecutionSubmitService kafkaSubmitService;

    public RecordSubmitService(ProcessedRecordsDAO processedRecordsDAO, RecordExecutionSubmitService kafkaSubmitService) {
        this.processedRecordsDAO = processedRecordsDAO;
        this.kafkaSubmitService = kafkaSubmitService;
    }

    /**
     * Submits record to storm cluster by sending it to valid Kafka topic.
     *
     * @param record
     * @param submitParameters
     * @return true if task size should be incremented and false if not. False is returned only in case of duplicated
     * record ids. For such cases record is sent to storm only once and excepting retries also only once performed in it.
     * So increasing task size for every duplicate occurence causes that task size would be bigger than number of records
     * performd in storm cluster and such task would be never marked as finished.
     * finish.
     */
    public boolean submitRecord(DpsRecord record, SubmitTaskParameters submitParameters) {
        Optional<ProcessedRecord> alreadySubmittedRecord = processedRecordsDAO.selectByPrimaryKey(record.getTaskId(), record.getRecordId());

        if (alreadySubmittedRecord.isEmpty()) {
            kafkaSubmitService.submitRecord(record, submitParameters.getTopicName());
            LOGGER.debug("Updating record in processed_records table: {}", record);
            processedRecordsDAO.insert(record.getTaskId(), record.getRecordId(), 0,
                    "", submitParameters.getTopologyName(), RecordState.QUEUED.toString(), "", "");
            return true;
        } else if (isResendingAfterFail(alreadySubmittedRecord.get(), submitParameters)) {
            LOGGER.info("Omitting record already sent to Kafka {}", record);
            processedRecordsDAO.updateStartTime(record.getTaskId(), record.getRecordId(), new Date());
            return true;
        } else {
            LOGGER.warn("Omitting duplicated record {}", record);
            return false;
        }

    }

    private boolean isResendingAfterFail(ProcessedRecord alreadySubmittedRecord, SubmitTaskParameters submitParameters) {
        Date currentExecutionStart = submitParameters.getStartTime();
        return submitParameters.isRestarted() && alreadySubmittedRecord.getStarTime().before(currentExecutionStart);
    }

}
