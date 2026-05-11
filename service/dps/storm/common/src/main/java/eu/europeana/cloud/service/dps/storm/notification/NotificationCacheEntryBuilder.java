package eu.europeana.cloud.service.dps.storm.notification;

import eu.europeana.cloud.service.dps.storm.ErrorType;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.dao.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.dao.NotificationsDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

public class NotificationCacheEntryBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(NotificationCacheEntryBuilder.class);

  private final CassandraTaskInfoDAO taskInfoDAO;
  private final CassandraTaskErrorsDAO taskErrorDAO;

  public NotificationCacheEntryBuilder(
      NotificationsDAO subTaskInfoDAO,
      CassandraTaskInfoDAO taskInfoDAO,
      CassandraTaskErrorsDAO taskErrorDAO) {

    this.taskInfoDAO = taskInfoDAO;
    this.taskErrorDAO = taskErrorDAO;
  }

  public NotificationCacheEntry build(long taskId) {
    var taskInfo = taskInfoDAO.findById(taskId).orElseThrow();
    // Processed records now consist only of success, duplicate, unchanged, fail records.
    // Depublish records are stored separately.
    // So to account for all processed records including depublish we need to add them together
    int processed = taskInfo.getProcessedRecords() + taskInfo.getProcessedDepublishRecords();
    NotificationCacheEntry.NotificationCacheEntryBuilder builder = NotificationCacheEntry.builder();
    builder.totalProcessed(processed);
    builder.errorTypes(new HashMap<>());
    if (processed > 0) {
      builder.successRecords(taskInfo.getSuccessRecords());
      builder.warningRecords(taskInfo.getWarningRecords());
      builder.failRecords(taskInfo.getFailRecords());
      builder.successDepublishRecords(taskInfo.getSuccessDepublishRecords());
      builder.failDepublishRecords(taskInfo.getFailDepublishRecords());
      builder.processedDepublishRecords(taskInfo.getProcessedDepublishRecords());
      builder.duplicateRecords(taskInfo.getDuplicateRecords());
      builder.unchangedRecords(taskInfo.getUnchangedRecords());
      builder.processedRecords(taskInfo.getProcessedRecords());
      builder.errorTypes(getMessagesUUIDsMap(taskId));
    }
    NotificationCacheEntry result = builder.build();
    LOGGER.info("Updated state of NotificationBolt from Cassandra for taskId={} counters={}",
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
