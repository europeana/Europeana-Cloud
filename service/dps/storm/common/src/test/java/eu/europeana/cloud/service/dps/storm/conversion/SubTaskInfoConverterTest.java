package eu.europeana.cloud.service.dps.storm.conversion;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.common.model.dps.Notification;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.service.dps.storm.dao.NotificationsDAO;
import java.util.HashMap;
import org.junit.Before;
import org.junit.Test;

public class SubTaskInfoConverterTest {


  public static final HashMap<String, String> ADDITIONAL_INFORMATION = new HashMap<>();
  public static final int RESOURCE_NUM = 2;
  public static final String RESOURCE = "some_resource";
  public static final String RESULT_RESOURCE = "some_result_resource";
  public static final RecordState STATE = RecordState.QUEUED;
  public static final String DESCRIPTION_KEY_VALUE = "some_description_key";
  public static final String EUROPEANA_ID_KEY_VALUE = "some_europeana_id_key";
  public static final String TIME_KEY_VALUE = "1112";

  Notification notification = mock(Notification.class);


  @Before
  public void init() {
    ADDITIONAL_INFORMATION.put(NotificationsDAO.STATE_DESCRIPTION_KEY, DESCRIPTION_KEY_VALUE);
    ADDITIONAL_INFORMATION.put(NotificationsDAO.EUROPEANA_ID_KEY, EUROPEANA_ID_KEY_VALUE);
    ADDITIONAL_INFORMATION.put(NotificationsDAO.PROCESSING_TIME_KEY, TIME_KEY_VALUE);
    when(notification.getResourceNum()).thenReturn(RESOURCE_NUM);
    when(notification.getResource()).thenReturn(RESOURCE);
    when(notification.getState()).thenReturn(STATE.toString());
    when(notification.getAdditionalInformation()).thenReturn(ADDITIONAL_INFORMATION);
    when(notification.getResultResource()).thenReturn(RESULT_RESOURCE);
  }

  @Test
  public void shouldProperlyConvertFromNotification() {
    SubTaskInfo taskInfo = SubTaskInfoConverter.fromNotification(notification);
    assertEquals(RESOURCE_NUM, taskInfo.getResourceNum());
    assertEquals(RESOURCE, taskInfo.getResource());
    assertEquals(RecordState.QUEUED, taskInfo.getRecordState());
    assertEquals(RESULT_RESOURCE, taskInfo.getResultResource());
    assertEquals(ADDITIONAL_INFORMATION.get(NotificationsDAO.STATE_DESCRIPTION_KEY), taskInfo.getAdditionalInformations());
    assertEquals(ADDITIONAL_INFORMATION.get(NotificationsDAO.EUROPEANA_ID_KEY), taskInfo.getEuropeanaId());
    assertEquals(Long.parseLong(ADDITIONAL_INFORMATION.get(NotificationsDAO.PROCESSING_TIME_KEY)), taskInfo.getProcessingTime());
  }
}