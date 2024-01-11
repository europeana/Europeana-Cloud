package eu.europeana.cloud.service.dps.storm.topologies.xslt.bolt;

import static eu.europeana.cloud.service.dps.test.TestConstants.CLOUD_ID;
import static eu.europeana.cloud.service.dps.test.TestConstants.REPRESENTATION_NAME;
import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE;
import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE_VERSION_URL;
import static eu.europeana.cloud.service.dps.test.TestConstants.VERSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Charsets;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.io.IOUtils;
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

public class XsltBoltTest {

  private static final String EXAMPLE_METIS_DATASET_ID = "100";
  private final int TASK_ID = 1;
  private final String TASK_NAME = "TASK_NAME";

  @Mock(name = "outputCollector")
  private OutputCollector outputCollector;


  @InjectMocks
  private XsltBolt xsltBolt = new XsltBolt(new CassandraProperties());

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
    xsltBolt.prepare();
  }

  @Test
  public void executeBolt() throws IOException {
    Tuple anchorTuple = mock(TupleImpl.class);
    String sampleXmlFileName = "/xmlForTesting.xml";
    StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, readMockContentOfURL(sampleXmlFileName),
        prepareStormTaskTupleParameters(), new Revision());
    xsltBolt.execute(anchorTuple, tuple);
    when(outputCollector.emit(any(Tuple.class), anyList())).thenReturn(null);
    verify(outputCollector, times(1)).emit(Mockito.any(Tuple.class), captor.capture());
    assertThat(captor.getAllValues().size(), is(1));
    List<Values> allValues = captor.getAllValues();
    assertEmittedTuple(allValues, 4);
  }

  @Captor
  ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

  @Test
  public void executeBoltWithInjection() throws IOException {
    Tuple anchorTuple = mock(TupleImpl.class);
    HashMap<String, String> parameters = prepareStormTaskTupleParameters();
    parameters.put(PluginParameterKeys.METIS_DATASET_ID, EXAMPLE_METIS_DATASET_ID);

    String injectXmlFileName = "/xmlForTestingParamInjection.xml";
    StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, readMockContentOfURL(injectXmlFileName),
        parameters, new Revision());
    xsltBolt.execute(anchorTuple, tuple);
    when(outputCollector.emit(any(Tuple.class), anyList())).thenReturn(null);
    verify(outputCollector, times(1)).emit(Mockito.any(Tuple.class), captor.capture());
    assertThat(captor.getAllValues().size(), is(1));
    List<Values> allValues = captor.getAllValues();
    assertEmittedTuple(allValues, 4);

    String transformed = new String((byte[]) allValues.get(0).get(3));
    assertNotNull(transformed);
    assertTrue(transformed.contains(EXAMPLE_METIS_DATASET_ID));
  }

  private HashMap<String, String> prepareStormTaskTupleParameters() {
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(PluginParameterKeys.XSLT_URL, "https://metis-core-rest.test.eanadev.org/datasets/xslt/default");
    parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
    return parameters;
  }

  //This is a mock content for the FILE URL
  private byte[] readMockContentOfURL(String xmlFile) throws IOException {
    return readFile(xmlFile);
  }

  private void assertEmittedTuple(List<Values> allValues, int expectedParametersSize) {
    assertNotNull(allValues);
    assertEquals(1, allValues.size());

    //parameters assertion
    assertTrue(allValues.get(0).get(4) instanceof Map);
    Map<String, String> parameters = ((Map<String, String>) allValues.get(0).get(4));
    assertNotNull(parameters);
    assertEquals(parameters.size(), expectedParametersSize);
    String cloudId = parameters.get(PluginParameterKeys.CLOUD_ID);
    assertNotNull(cloudId);
    assertEquals(cloudId, SOURCE + CLOUD_ID);
    String representationName = parameters.get(PluginParameterKeys.REPRESENTATION_NAME);
    assertNotNull(representationName);
    assertEquals(representationName, SOURCE + REPRESENTATION_NAME);
    String version = parameters.get(PluginParameterKeys.REPRESENTATION_VERSION);
    assertNotNull(version);
    assertEquals(version, SOURCE + VERSION);
  }

  private byte[] readFile(String fileName) throws IOException {
    Optional<URL> optResource = Optional.ofNullable(getClass().getResource(fileName));
    String myXml = IOUtils.toString(optResource.orElseThrow(FileNotFoundException::new),
        Charsets.UTF_8);
    byte[] bytes = myXml.getBytes(StandardCharsets.UTF_8);
    InputStream contentStream = new ByteArrayInputStream(bytes);
    return IOUtils.toByteArray(contentStream);
  }
}