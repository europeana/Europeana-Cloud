package eu.europeana.cloud.service.dps.storm.notification;

import eu.europeana.cloud.service.dps.storm.ErrorType;
import eu.europeana.cloud.service.dps.storm.dao.NotificationsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

public class NotificationEntryCacheBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationEntryCacheBuilder.class);

    private final NotificationsDAO subTaskInfoDAO;
    private final CassandraTaskInfoDAO taskInfoDAO;
    private final CassandraTaskErrorsDAO taskErrorDAO;

    public NotificationEntryCacheBuilder(
            NotificationsDAO subTaskInfoDAO,
            CassandraTaskInfoDAO taskInfoDAO,
            CassandraTaskErrorsDAO taskErrorDAO) {

        this.subTaskInfoDAO = subTaskInfoDAO;
        this.taskInfoDAO = taskInfoDAO;
        this.taskErrorDAO = taskErrorDAO;
    }

    public NotificationCacheEntry build(long taskId) {

        NotificationCacheEntry.NotificationCacheEntryBuilder builder = NotificationCacheEntry.builder();

        int processed = subTaskInfoDAO.getProcessedFilesCount(taskId);

        builder.processed(processed);
        builder.errorTypes(new HashMap<>());
        var taskInfo = taskInfoDAO.findById(taskId).orElseThrow();
        builder.expectedRecordsNumber(taskInfo.getExpectedRecordsNumber());
        if (processed > 0) {
            builder.processedRecordsCount(taskInfo.getProcessedRecordsCount());
            builder.ignoredRecordsCount(taskInfo.getIgnoredRecordsCount());
            builder.deletedRecordsCount(taskInfo.getDeletedRecordsCount());
            builder.processedErrorsCount(taskInfo.getProcessedErrorsCount());
            builder.deletedErrorsCount(taskInfo.getDeletedErrorsCount());
            builder.errorTypes(getMessagesUUIDsMap(taskId));
        }
        NotificationCacheEntry result = builder.build();
        LOGGER.info("Restored state of NotificationBolt from Cassandra for taskId={} counters={}",
                taskId, result);
        return result;
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
