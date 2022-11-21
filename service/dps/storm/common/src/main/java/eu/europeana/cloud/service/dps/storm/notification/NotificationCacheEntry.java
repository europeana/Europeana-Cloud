package eu.europeana.cloud.service.dps.storm.notification;

import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.storm.ErrorType;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
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
  private int processedRecordsCount;
  private int ignoredRecordsCount;
  private int deletedRecordsCount;
  private int processedErrorsCount;
  private int deletedErrorsCount;
  private int expectedRecordsNumber;
  Map<String, ErrorType> errorTypes;

  public void incrementCounters(NotificationTuple notificationTuple) {
    processed++;

    if (notificationTuple.isMarkedAsDeleted()) {
      deletedRecordsCount++;
      if (isErrorTuple(notificationTuple)) {
        deletedErrorsCount++;
      }
    } else if (notificationTuple.isIgnoredRecord()) {
      if (isErrorTuple(notificationTuple)) {
        LOGGER.error("Tuple is marked as ignored and error in the same time! It should not occur. Tuple: {}"
            , notificationTuple);
        processedRecordsCount++;
        processedErrorsCount++;
      } else {
        ignoredRecordsCount++;
      }
    } else {
      processedRecordsCount++;
      if (isErrorTuple(notificationTuple)) {
        processedErrorsCount++;
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
