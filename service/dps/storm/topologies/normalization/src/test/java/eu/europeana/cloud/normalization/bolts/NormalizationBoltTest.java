package eu.europeana.cloud.normalization.bolts;

import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE_VERSION_URL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class NormalizationBoltTest {

  @Mock(name = "outputCollector")
  private OutputCollector outputCollector;

  @Captor
  ArgumentCaptor<Values> captor;

  @InjectMocks
  private NormalizationBolt normalizationBolt = new NormalizationBolt(new CassandraProperties());

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void shouldNormalizeRecord() throws Exception {
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
    assertEquals(new String(expected), new String((byte[]) capturedValues.get(3)).replaceAll("\r", ""));
  }


  @Test
  @SuppressWarnings("unchecked")
  public void shouldEmitErrorWhenNormalizationResultContainsErrorMessage() throws Exception {
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
  public void shouldEmitErrorWhenCantPrepareTupleForEmission() throws Exception {
    //given
    Tuple anchorTuple = mock(TupleImpl.class);
    byte[] inputData = Files.readAllBytes(Paths.get("src/test/resources/edm.xml"));
    normalizationBolt.prepare();

    //when
    normalizationBolt.execute(anchorTuple, getMalformedStormTuple(inputData));

    //then
    Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.anyString(), any(Tuple.class), captor.capture());
    var val = (Map<String, String>) captor.getValue().get(1);
    assertTrue(val.get(NotificationParameterKeys.STATE_DESCRIPTION).contains("Cannot prepare output storm tuple."));
    assertTrue(val.get(NotificationParameterKeys.STATE_DESCRIPTION).contains("malformed.url"));
  }

  private StormTaskTuple getCorrectStormTuple(byte[] inputData) {
    return getStormTuple(SOURCE_VERSION_URL, inputData);
  }

  private StormTaskTuple getMalformedStormTuple(byte[] inputData) {
    return getStormTuple("malformed.url", inputData);
  }

  private StormTaskTuple getStormTuple(String fileUrl, byte[] inputData) {
    return new StormTaskTuple(123, "TASK_NAME", fileUrl, inputData, prepareStormTaskTupleParameters(), null);
  }

  private HashMap<String, String> prepareStormTaskTupleParameters() {
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
    return parameters;
  }
}