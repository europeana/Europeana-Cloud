package eu.europeana.cloud.service.dps.storm.conversion;

import eu.europeana.cloud.common.model.dps.Notification;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.service.dps.storm.dao.NotificationsDAO;
import java.util.Map;

/**
 * Class converting from DB rows or other class instances to SubTaskInfo class instance
 */
public final class SubTaskInfoConverter {

  private SubTaskInfoConverter() {
  }


  /**
   * Converts Notification class instance to the {@link SubTaskInfo} class instance.
   *
   * @param notification notification instance that will be converted
   * @return {@link SubTaskInfo} class instance generated based on the provided notification class instance.
   */
  public static SubTaskInfo fromNotification(Notification notification) {
    Map<String, String> additionalInformation = notification.getAdditionalInformation();
    return new SubTaskInfo(
        notification.getResourceNum(),
        notification.getResource(),
        RecordState.valueOf(notification.getState()),
        notification.getInfoText(),
        additionalInformation.get(NotificationsDAO.STATE_DESCRIPTION_KEY),
        additionalInformation.get(NotificationsDAO.EUROPEANA_ID_KEY),
        additionalInformation.get(NotificationsDAO.PROCESSING_TIME_KEY) !=
            null ? Long.parseLong(additionalInformation.get(NotificationsDAO.PROCESSING_TIME_KEY)) : 0L,
        notification.getResultResource()
    );
  }
}
