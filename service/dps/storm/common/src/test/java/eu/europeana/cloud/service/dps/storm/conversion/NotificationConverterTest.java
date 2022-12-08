package eu.europeana.cloud.service.dps.storm.conversion;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Row;
import eu.europeana.cloud.common.model.dps.Notification;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;
import java.util.HashMap;
import org.junit.Before;
import org.junit.Test;

public class NotificationConverterTest {

  public static final HashMap<String, String> ADDITIONAL_INFORMATION = new HashMap<>();
  public static final int BUCKET_NUMBER = 1;
  public static final int RESOURCE_NUM = 2;
  public static final String INFO_TEXT = "some_info_text";
  public static final String RESOURCE = "some_resource";
  public static final String RESULT_RESOURCE = "some_result_resource";
  public static final String STATE = "some_state";
  public static final String TOPOLOGY_NAME = "some_topology_name";
  private static final long TASK_ID = 111L;
  Row row = mock(Row.class);


  @Before
  public void init() {
    ADDITIONAL_INFORMATION.put("some_key", "some_value");
    when(row.getLong(CassandraTablesAndColumnsNames.NOTIFICATION_TASK_ID)).thenReturn(TASK_ID);
    when(row.getInt(CassandraTablesAndColumnsNames.NOTIFICATION_BUCKET_NUMBER)).thenReturn(BUCKET_NUMBER);
    when(row.getInt(CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE_NUM)).thenReturn(RESOURCE_NUM);
    when(row.getMap(CassandraTablesAndColumnsNames.NOTIFICATION_ADDITIONAL_INFORMATION, String.class, String.class)).thenReturn(
        ADDITIONAL_INFORMATION);
    when(row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_INFO_TEXT)).thenReturn(INFO_TEXT);
    when(row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_RESOURCE)).thenReturn(RESOURCE);
    when(row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_RESULT_RESOURCE)).thenReturn(RESULT_RESOURCE);
    when(row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_STATE)).thenReturn(STATE);
    when(row.getString(CassandraTablesAndColumnsNames.NOTIFICATION_TOPOLOGY_NAME)).thenReturn(TOPOLOGY_NAME);

  }

  @Test
  public void fromDBRow() {
    Notification notification = NotificationConverter.fromDBRow(row);
    assertEquals(TASK_ID, notification.getTaskId());
    assertEquals(BUCKET_NUMBER, notification.getBucketNumber());
    assertEquals(RESOURCE_NUM, notification.getResourceNum());
    assertEquals(ADDITIONAL_INFORMATION, notification.getAdditionalInformation());
    assertEquals(INFO_TEXT, notification.getInfoText());
    assertEquals(RESOURCE, notification.getResource());
    assertEquals(RESULT_RESOURCE, notification.getResultResource());
    assertEquals(STATE, notification.getState());
    assertEquals(TOPOLOGY_NAME, notification.getTopologyName());
  }
}