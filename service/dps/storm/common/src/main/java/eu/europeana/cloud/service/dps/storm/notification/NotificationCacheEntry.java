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

    boolean depublished = notificationTuple.isMarkedAsDepublished();

    if (isErrorTuple(notificationTuple)) {
      if (depublished) {
        depublishedProcessedRecords++;
        depublishedFailRecords++;
      } else {
        processedRecords++;
        failRecords++;
      }

      LOGGER.error("Tuple is marked as error!");
      return;
    }

    if (depublished) {
      depublishedProcessedRecords++;
      depublishedSuccessRecords++;
      return;
    }

    processedRecords++;

    if (notificationTuple.isUnchangedRecord()) {
      unchangedRecords++;
      return;
    }

    if (notificationTuple.isDuplicatedRecord()) {
      duplicateRecords++;
      return;
    }

    boolean hasWarnings = notificationTuple.getReportSet()
            .stream()
            .anyMatch(report -> report.getMessageType() == Type.WARN);

    if (hasWarnings) {
      warningRecords++;
    }

    successRecords++;
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
