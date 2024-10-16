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
    return SubTaskInfo.builder()
                      .resourceNum(notification.getResourceNum())
                      .resource(notification.getResource())
                      .recordState(RecordState.valueOf(notification.getState()))
                      .info(notification.getInfoText())
                      .additionalInformations(additionalInformation.get(NotificationsDAO.STATE_DESCRIPTION_KEY))
                      .europeanaId(additionalInformation.get(NotificationsDAO.EUROPEANA_ID_KEY))
                      .processingTime(additionalInformation.get(NotificationsDAO.PROCESSING_TIME_KEY) !=
                          null ? Long.parseLong(additionalInformation.get(NotificationsDAO.PROCESSING_TIME_KEY)) : 0L)
                      .resultResource(notification.getResultResource()).build();
  }
}
