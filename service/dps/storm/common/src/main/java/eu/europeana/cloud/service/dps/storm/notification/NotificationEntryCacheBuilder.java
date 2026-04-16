package eu.europeana.cloud.service.dps.storm.notification;

import eu.europeana.cloud.common.model.dps.EngineTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
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

import static eu.europeana.cloud.common.model.dps.TaskInfo.UNKNOWN_EXPECTED_RECORDS_NUMBER;

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
    var taskInfo = taskInfoDAO.findById(taskId).orElseThrow();
    // Now success records doesn't contain unchanged or duplicated or marked as deleted records
    // So total number of processed records should add up to: success + duplicate + unchanged + fail + deleted
    int processed = taskInfo.getProcessedRecords() + taskInfo.getProcessedDepublishRecords();
    NotificationCacheEntry.NotificationCacheEntryBuilder builder = NotificationCacheEntry.builder();
    builder.processed(processed);
    builder.errorTypes(new HashMap<>());
    builder.expectedRecords(evaluateCredibleExpectedRecordNumber(taskInfo));
    if (processed > 0) {
      builder.successRecords(taskInfo.getSuccessRecords());
      builder.warningRecords(taskInfo.getWarningRecords());
      builder.failRecords(taskInfo.getFailRecords());
      builder.depublishedSuccessRecords(taskInfo.getSuccessDepublishRecords());
      builder.depublishedFailRecords(taskInfo.getFailDepublishRecords());
      builder.depublishedProcessedRecords(taskInfo.getProcessedDepublishRecords());
      builder.duplicateRecords(taskInfo.getDuplicateRecords());
      builder.unchangedRecords(taskInfo.getUnchangedRecords());
      builder.processedRecords(taskInfo.getProcessedRecords());
      builder.expectedRecords(taskInfo.getExpectedRecords());
      builder.errorTypes(getMessagesUUIDsMap(taskId));
    }
    NotificationCacheEntry result = builder.build();
    LOGGER.info("Updated state of NotificationBolt from Cassandra for taskId={} counters={}",
        taskId, result);
    return result;
  }

  private int evaluateCredibleExpectedRecordNumber(TaskInfo taskInfo) {
    return taskInfo.getEngineTaskState() == EngineTaskState.QUEUED ? taskInfo.getExpectedRecords() : UNKNOWN_EXPECTED_RECORDS_NUMBER;
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
