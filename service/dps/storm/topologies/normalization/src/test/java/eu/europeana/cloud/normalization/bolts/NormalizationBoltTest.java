package eu.europeana.cloud.normalization.bolts;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.cloud.service.dps.storm.tuple.common.RecordData;
import eu.europeana.cloud.service.dps.storm.tuple.common.TaskData;
import eu.europeana.cloud.service.dps.storm.tuple.common.processingData;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE_VERSION_URL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class NormalizationBoltTest {

  @Mock(name = "outputCollector")
  private OutputCollector outputCollector;

  @Captor
  ArgumentCaptor<Values> captor;

  @InjectMocks
  private NormalizationBolt normalizationBolt = new NormalizationBolt(new CassandraProperties());

  @Test
  void shouldNormalizeRecord() throws Exception {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);
    byte[] inputData = Files.readAllBytes(Paths.get("src/test/resources/edm.xml"));
    byte[] expected = Files.readAllBytes(Paths.get("src/test/resources/normalized.xml"));
    normalizationBolt.prepare();

    //when
    normalizationBolt.execute(anchorTuple, getCorrectStormTuple(inputData));

    //then
    Mockito.verify(outputCollector, Mockito.times(1)).emit(any(Tuple.class), captor.capture());
    Values capturedValues = captor.getValue();
    assertEquals(new String(expected), new String(((RecordData) capturedValues.get(5)).getFileData()).replaceAll("\r", ""));
  }


  @Test
  @SuppressWarnings("unchecked")
  void shouldEmitErrorWhenNormalizationResultContainsErrorMessage() throws Exception {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);
    byte[] inputData = Files.readAllBytes(Paths.get("src/test/resources/edm-not-valid.xml"));
    normalizationBolt.prepare();

    //when
    normalizationBolt.execute(anchorTuple, getCorrectStormTuple(inputData));

    //then
    Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.anyString(), any(Tuple.class), captor.capture());
    var val = (Map<String, String>) captor.getValue().get(1);
    assertTrue(val.get(NotificationParameterKeys.STATE_DESCRIPTION).startsWith("Error during normalization."));
    assertTrue(val.get(NotificationParameterKeys.INFO_TEXT).startsWith("Issue converting record String to Document"));
  }


  @Test
  @SuppressWarnings("unchecked")
  void shouldEmitErrorWhenCantPrepareTupleForEmission() throws Exception {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);
    byte[] inputData = Files.readAllBytes(Paths.get("src/test/resources/edm.xml"));
    normalizationBolt.prepare();

    //when
    normalizationBolt.execute(anchorTuple, getMalformedStormTuple(inputData));

    //then
    Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.anyString(), any(Tuple.class), captor.capture());
    var val = (Map<String, String>) captor.getValue().get(1);
    assertTrue(val.get(NotificationParameterKeys.STATE_DESCRIPTION).contains("Cannot prepare output common tuple."));
    assertTrue(val.get(NotificationParameterKeys.STATE_DESCRIPTION).contains("malformed.url"));
  }

  private CommonTaskTuple getCorrectStormTuple(byte[] inputData) {
    return getStormTuple(SOURCE_VERSION_URL, inputData);
  }

  private CommonTaskTuple getMalformedStormTuple(byte[] inputData) {
    return getStormTuple("malformed.url", inputData);
  }

  private CommonTaskTuple getStormTuple(String fileUrl, byte[] inputData) {
    var tuple = new CommonTaskTuple(
            new TaskData(123, "TASK_NAME"),
            new RecordData(fileUrl, inputData, true),
            new processingData());
    tuple.setParameters(prepareStormTaskTupleParameters());
    return tuple;
  }

  private HashMap<String, String> prepareStormTaskTupleParameters() {
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
    return parameters;
  }
}