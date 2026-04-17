package eu.europeana.cloud.service.dps.storm.notification;

import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.storm.ErrorType;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.tuple.notification.NotificationTuple;
import eu.europeana.enrichment.rest.client.report.Type;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@Getter
@Builder
@ToString
public class NotificationCacheEntry {

  private static final Logger LOGGER = LoggerFactory.getLogger(NotificationCacheEntry.class);

  private int processed;
  private int successRecords;
  private int warningRecords;
  private int failRecords;
  private int depublishedSuccessRecords;
  private int depublishedFailRecords;
  private int depublishedProcessedRecords;
  private int duplicateRecords;
  private int unchangedRecords;
  private int processedRecords;
  private int expectedRecords;
  Map<String, ErrorType> errorTypes;

  public void incrementCounters(NotificationTuple notificationTuple) {
    processed++;

    if (notificationTuple.isMarkedAsDepublished()) {
      if (isErrorTuple(notificationTuple)) {
        LOGGER.error("Tuple is marked as deleted and error in the same time! It should not occur. Tuple: {}"
                , notificationTuple);
        depublishedFailRecords++;
      } else {
        depublishedSuccessRecords++;
      }
      depublishedProcessedRecords++;
    } else if (notificationTuple.isUnchangedRecord()) {
      if (isErrorTuple(notificationTuple)) {
        LOGGER.error("Tuple is marked as ignored and error in the same time! It should not occur. Tuple: {}"
                , notificationTuple);
      } else {
        unchangedRecords++;
        processedRecords++;
      }
    } else if (notificationTuple.isDuplicatedRecord()) {
      if (isErrorTuple(notificationTuple)) {
        LOGGER.error("Tuple is marked as duplicate and error in the same time! It should not occur. Tuple: {}"
                , notificationTuple);
      } else {
        duplicateRecords++;
        processedRecords++;
      }
    } else {
      processedRecords++;
      if (isErrorTuple(notificationTuple)) {
        failRecords++;
      } else {
        if (notificationTuple.getReportSet()
                .stream().anyMatch(report ->
                        report.getMessageType() == Type.WARN)) {
          warningRecords++;
        }
        successRecords++;
      }
    }
  }

  private boolean isErrorTuple(NotificationTuple notificationTuple) {
    return String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.STATE))
                 .equalsIgnoreCase(RecordState.ERROR.toString());
  }

  public ErrorType getErrorType(String infoText) {
    return errorTypes.computeIfAbsent(infoText,
        key -> ErrorType.builder()
                        .uuid(new com.eaio.uuid.UUID().toString())
                        .build());
  }
}
