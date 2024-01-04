package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

public final class DiagnosticContextWrapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(DiagnosticContextWrapper.class);

  private static final String TASK_ID_FIELD_NAME = "task_id";
  private static final String RECORD_ID_FIELD_NAME = "record_id";
  private static final String RECORD_DELETED_FIELD_NAME = "deleted";

  private DiagnosticContextWrapper() {
  }

  public static void putValuesFrom(StormTaskTuple stormTaskTuple) {
    MDC.put(TASK_ID_FIELD_NAME, String.valueOf(stormTaskTuple.getTaskId()));
    MDC.put(RECORD_ID_FIELD_NAME, String.valueOf(stormTaskTuple.getFileUrl()));
  }

  public static void putValuesFrom(NotificationTuple notificationTuple) {
    MDC.put(TASK_ID_FIELD_NAME, String.valueOf(notificationTuple.getTaskId()));
    MDC.put(RECORD_ID_FIELD_NAME, String.valueOf(notificationTuple.getResource()));
  }

  public static void putValuesFrom(DpsRecord dpsRecord) {
    MDC.put(TASK_ID_FIELD_NAME, String.valueOf(dpsRecord.getTaskId()));
    MDC.put(RECORD_ID_FIELD_NAME, String.valueOf(dpsRecord.getRecordId()));
    MDC.put(RECORD_DELETED_FIELD_NAME, Boolean.toString(dpsRecord.isMarkedAsDeleted()));
  }

  public static void clear() {
    LOGGER.trace("Cleaning diagnostic context");
    MDC.clear();
  }
}
