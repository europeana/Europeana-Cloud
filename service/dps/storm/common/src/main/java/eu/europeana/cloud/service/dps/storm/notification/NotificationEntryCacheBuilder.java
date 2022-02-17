package eu.europeana.cloud.service.dps.storm.notification;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.storm.ErrorType;
import eu.europeana.cloud.service.dps.storm.dao.CassandraSubTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static eu.europeana.cloud.common.model.dps.TaskInfo.UNKNOWN_EXPECTED_RECORD_NUMBER;

public class NotificationEntryCacheBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationEntryCacheBuilder.class);

    private final CassandraSubTaskInfoDAO subTaskInfoDAO;
    private final CassandraTaskInfoDAO taskInfoDAO;
    private final CassandraTaskErrorsDAO taskErrorDAO;

    public NotificationEntryCacheBuilder(
            CassandraSubTaskInfoDAO subTaskInfoDAO,
            CassandraTaskInfoDAO taskInfoDAO,
            CassandraTaskErrorsDAO taskErrorDAO) {

        this.subTaskInfoDAO = subTaskInfoDAO;
        this.taskInfoDAO = taskInfoDAO;
        this.taskErrorDAO = taskErrorDAO;
    }

    public NotificationCacheEntry build(long taskId) {
        var taskInfo = taskInfoDAO.findById(taskId).orElseThrow();
        int processed = taskInfo.getProcessedRecordsCount() + taskInfo.getIgnoredRecordsCount() + taskInfo.getDeletedRecordsCount();
        NotificationCacheEntry.NotificationCacheEntryBuilder builder = NotificationCacheEntry.builder();
        builder.processed(processed);
        builder.errorTypes(new HashMap<>());
        builder.expectedRecordsNumber(evaluateCredibleExpectedRecordNumber(taskInfo));
        if (processed > 0) {
            builder.processedRecordsCount(taskInfo.getProcessedRecordsCount());
            builder.ignoredRecordsCount(taskInfo.getIgnoredRecordsCount());
            builder.deletedRecordsCount(taskInfo.getDeletedRecordsCount());
            builder.processedErrorsCount(taskInfo.getProcessedErrorsCount());
            builder.deletedErrorsCount(taskInfo.getDeletedErrorsCount());
            builder.errorTypes(getMessagesUUIDsMap(taskId));
        }
        NotificationCacheEntry result = builder.build();
        LOGGER.info("Updated state of NotificationBolt from Cassandra for taskId={} counters={}",
                taskId, result);
        return result;
    }

    private int evaluateCredibleExpectedRecordNumber(TaskInfo taskInfo) {
        return taskInfo.getState() == TaskState.QUEUED ? taskInfo.getExpectedRecordsNumber() : UNKNOWN_EXPECTED_RECORD_NUMBER;
    }

    private Map<String, ErrorType> getMessagesUUIDsMap(long taskId) {
        Map<String, ErrorType> errorMessageToUuidMap = new HashMap<>();
        Iterator<ErrorType> it = taskErrorDAO.getAll(taskId);
        while (it.hasNext()) {
            ErrorType errorType = it.next();
            Optional<String> message = taskErrorDAO.getErrorMessage(taskId, errorType.getUuid());

            message.ifPresent(s -> errorMessageToUuidMap.put(s,
                    ErrorType.builder()
                            .taskId(taskId)
                            .uuid(errorType.getUuid())
                            .message(s)
                            .count(errorType.getCount()).build()));
        }
        return errorMessageToUuidMap;
    }
}
