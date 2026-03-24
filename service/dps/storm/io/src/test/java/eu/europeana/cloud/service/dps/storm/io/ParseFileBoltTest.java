package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.tuple.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.tuple.storm.TaskMetadata;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ParseFileBoltTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParseFileBoltTest.class);

  private static final String FILE_URL = "FILE_URL";
  private static final String NOTIFICATION_STREAM_NAME = "NotificationStream";
  private static final long TASK_ID = 1;


  @Mock(name = "outputCollector")
  private OutputCollector outputCollector;

  @Mock(name = "fileClient")
  private FileServiceClient fileClient;

  @Mock(name = "taskStatusChecker")
  private TaskStatusChecker taskStatusChecker;

  @Captor
  ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);


  @InjectMocks
  static ParseFileForMediaBolt parseFileBolt =
          new ParseFileForMediaBolt(new CassandraProperties(), "localhost/mcs", "user", "password");

  private StormTaskTuple stormTaskTuple;
  private static List<String> expectedParametersKeysList;

  @BeforeAll
  static void init() {

    parseFileBolt.prepare();
    expectedParametersKeysList = Arrays.asList(
            PluginParameterKeys.RESOURCE_LINK_KEY,
            PluginParameterKeys.RESOURCE_URL,
            PluginParameterKeys.RESOURCE_LINKS_COUNT,
            PluginParameterKeys.MAIN_THUMBNAIL_AVAILABLE,
            PluginParameterKeys.RESOURCE_LINK_KEY);

  }

  @BeforeEach
  void prepareTuple() {
    stormTaskTuple = new StormTaskTuple();
    stormTaskTuple.setTaskId(TASK_ID);
    stormTaskTuple.setRecordUri(FILE_URL);
    stormTaskTuple.setRecordUri(FILE_URL);
    stormTaskTuple.setMessageProcessingStartTimeInMs(1);
    stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, "3");
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldParseFileAndEmitResources() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);

    try (InputStream stream = this.getClass().getResourceAsStream("/files/Item_35834473.xml")) {
      when(fileClient.getFile(FILE_URL)).thenReturn(stream);
      when(taskStatusChecker.hasDroppedStatus(TASK_ID)).thenReturn(false);
      parseFileBolt.execute(anchorTuple, stormTaskTuple);
      verify(outputCollector, Mockito.times(4)).emit(any(Tuple.class), captor.capture()); // 4 hasView, 1 edm:object

      List<Values> capturedValuesList = captor.getAllValues();
      assertEquals(4, capturedValuesList.size());
      for (Values values : capturedValuesList) {
        assertEquals(6, values.size());
        assertTrue(values.get(3) instanceof TaskMetadata);
        var val = ((TaskMetadata) values.get(3)).getParameters();
        assertNotNull(val);
        for (String parameterKey : val.keySet()) {
          assertTrue(expectedParametersKeysList.contains(parameterKey));
        }
      }
    }
  }


  @Test
  void shouldDropTaskAndStopEmitting() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);

    try (InputStream stream = this.getClass().getResourceAsStream("/files/Item_35834473.xml")) {
      when(fileClient.getFile(FILE_URL)).thenReturn(stream);
      when(taskStatusChecker.hasDroppedStatus(TASK_ID)).thenReturn(false).thenReturn(false).thenReturn(true);
      parseFileBolt.execute(anchorTuple, stormTaskTuple);
      verify(outputCollector, Mockito.times(2)).emit(any(Tuple.class),
              captor.capture()); // 4 hasView, 1 edm:object, dropped after 2 resources
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldParseFileWithEmptyResourcesAndForwardOneTuple() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, "0");

    try (InputStream stream = this.getClass().getResourceAsStream("/files/no-resources.xml")) {
      when(fileClient.getFile(FILE_URL)).thenReturn(stream);
      parseFileBolt.execute(anchorTuple, stormTaskTuple);
      verify(outputCollector, Mockito.times(1)).emit(any(Tuple.class), captor.capture());
      Values values = captor.getValue();
      assertNotNull(values);
      LOGGER.info("{}", values);
      assertTrue(values.get(3) instanceof TaskMetadata);
      var map = ((TaskMetadata) values.get(3)).getParameters();
      LOGGER.info("{}", map);
      assertEquals(1, map.size());
      assertNotNull(map.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
      assertNull(map.get(PluginParameterKeys.RESOURCE_LINK_KEY));
    }

  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldEmitErrorWhenDownloadFileFails() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    doThrow(MCSException.class).when(fileClient).getFile(FILE_URL);
    parseFileBolt.execute(anchorTuple, stormTaskTuple);
    verify(outputCollector, Mockito.times(1)).emit(eq(NOTIFICATION_STREAM_NAME), any(Tuple.class), captor.capture());
    Values values = captor.getValue();
    assertNotNull(values);
    var valueMap = (Map<String, String>) values.get(1);
    assertNotNull(valueMap);
    assertTrue(
            valueMap.get(NotificationParameterKeys.STATE_DESCRIPTION).contains("Error while reading and parsing the EDM file"));
    assertEquals(RecordState.ERROR.toString(), valueMap.get(NotificationParameterKeys.STATE));
    assertNull(valueMap.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
    verify(outputCollector, Mockito.times(0)).emit(anyList());
  }


  @Test
  @SuppressWarnings("unchecked")
  void shouldEmitErrorWhenGettingResourceLinksFails() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    try (InputStream stream = this.getClass().getResourceAsStream("/files/broken.xml")) {
      when(fileClient.getFile(FILE_URL)).thenReturn(stream);
      parseFileBolt.execute(anchorTuple, stormTaskTuple);
      verify(outputCollector, Mockito.times(1)).emit(eq(NOTIFICATION_STREAM_NAME), any(Tuple.class), captor.capture());
      Values values = captor.getValue();
      assertNotNull(values);
      var valueMap = (Map<String, String>) values.get(1);
      assertNotNull(valueMap);
      assertTrue(
          valueMap.get(NotificationParameterKeys.STATE_DESCRIPTION).contains("Error while reading and parsing the EDM file"));
      assertEquals(RecordState.ERROR.toString(), valueMap.get(NotificationParameterKeys.STATE));
      assertNull(valueMap.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
      verify(outputCollector, Mockito.times(0)).emit(any(Tuple.class), anyList());
    }

  }
}
