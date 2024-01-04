package eu.europeana.cloud.service.dps.storm.notification.handler;

import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.notification.NotificationCacheEntry;

public final class NotificationHandlerConfigBuilder {

  private NotificationHandlerConfigBuilder() {
  }

  public static NotificationHandlerConfig prepareNotificationHandlerConfig(
      NotificationTuple notificationTuple,
      NotificationCacheEntry notificationCacheEntry) {

    NotificationHandlerConfig.NotificationHandlerConfigBuilder builder = NotificationHandlerConfig.builder();
    builder.recordStateToBeSet(isError(notificationTuple) ? RecordState.ERROR : RecordState.SUCCESS);
    builder.notificationCacheEntry(notificationCacheEntry);

    return builder.build();
  }

  private static boolean isError(NotificationTuple notificationTuple) {
    return isErrorTuple(notificationTuple)
        || (notificationTuple.getParameter(PluginParameterKeys.UNIFIED_ERROR_MESSAGE) != null);
  }

  private static boolean isErrorTuple(NotificationTuple notificationTuple) {
    return String.valueOf(notificationTuple.getParameters().get(NotificationParameterKeys.STATE))
                 .equalsIgnoreCase(RecordState.ERROR.toString());
  }
}
