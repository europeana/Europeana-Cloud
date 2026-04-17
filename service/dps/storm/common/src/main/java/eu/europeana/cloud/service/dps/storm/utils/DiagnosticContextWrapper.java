package eu.europeana.cloud.service.dps.storm.utils;

import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.cloud.service.dps.storm.tuple.notification.NotificationTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static eu.europeana.cloud.common.log.AttributePassingUtils.*;

public final class DiagnosticContextWrapper {

  private static final Logger LOGGER = LoggerFactory.getLogger(DiagnosticContextWrapper.class);

  private DiagnosticContextWrapper() {
  }

  public static void putValuesFrom(CommonTaskTuple commonTaskTuple) {
    MDC.put(TASK_ID_CONTEXT_ATTR, String.valueOf(commonTaskTuple.getTaskId()));
    MDC.put(RECORD_ID_CONTEXT_ATTR, String.valueOf(commonTaskTuple.getRecordUri()));
  }

  public static void putValuesFrom(NotificationTuple notificationTuple) {
    MDC.put(TASK_ID_CONTEXT_ATTR, String.valueOf(notificationTuple.getTaskId()));
    MDC.put(RECORD_ID_CONTEXT_ATTR, String.valueOf(notificationTuple.getResource()));
  }

  public static void putValuesFrom(DpsRecord dpsRecord) {
    MDC.put(TASK_ID_CONTEXT_ATTR, String.valueOf(dpsRecord.getTaskId()));
    MDC.put(RECORD_ID_CONTEXT_ATTR, String.valueOf(dpsRecord.getRecordId()));
    MDC.put(RECORD_DELETED_CONTEXT_ATTR, Boolean.toString(dpsRecord.isMarkedAsDepublished()));
  }

  public static void clear() {
    LOGGER.trace("Cleaning diagnostic context");
    MDC.clear();
  }
}
