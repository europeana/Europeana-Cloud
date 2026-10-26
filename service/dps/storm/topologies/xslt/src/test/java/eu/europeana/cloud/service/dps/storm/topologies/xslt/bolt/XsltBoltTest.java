package eu.europeana.cloud.service.dps.storm.topologies.xslt.bolt;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import com.google.common.base.Charsets;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.tuple.common.RecordData;
import eu.europeana.cloud.service.dps.storm.tuple.common.ProcessingData;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.cloud.service.dps.storm.tuple.common.TaskData;
import org.apache.commons.io.IOUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import static org.xmlunit.matchers.HasXPathMatcher.hasXPath;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class XsltBoltTest {

  private static final String EXAMPLE_METIS_DATASET_ID = "100";
  private final int TASK_ID = 1;
  private final String TASK_NAME = "TASK_NAME";

  private static final Map<String, String> NS = Map.of(
          "rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
          "edm", "http://www.europeana.eu/schemas/edm/",
          "dc",  "http://purl.org/dc/elements/1.1/",
          "ore", "http://www.openarchives.org/ore/terms/"
  );

  @Mock(name = "outputCollector")
  private OutputCollector outputCollector;

  @RegisterExtension
  static WireMockExtension wireMockExtension =
          WireMockExtension.newInstance()
                  .options(wireMockConfig().port(9876))
                  .build();

  @InjectMocks
  private XsltBolt xsltBolt = new XsltBolt(new CassandraProperties());

  @BeforeEach
  void init() {
    xsltBolt.prepare();
  }

  @Test
  void executeBolt() throws IOException {
    Tuple anchorTuple = mock(TupleImpl.class);
    String sampleXmlFileName = "/xmlForTesting.xml";
    CommonTaskTuple tuple = new CommonTaskTuple(
            new TaskData(TASK_ID, TASK_NAME),
            new RecordData(SOURCE_VERSION_URL, readMockContentOfURL(sampleXmlFileName), true),
            new ProcessingData());
    tuple.setParameters(prepareStormTaskTupleParameters());
    tuple.setOutputRevision(new Revision(REVISION_NAME, REVISION_PROVIDER, new Date()));
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
  void executeBoltWithInjection() throws IOException {
    Tuple anchorTuple = mock(TupleImpl.class);
    HashMap<String, String> parameters = prepareStormTaskTupleParameters();
    parameters.put(PluginParameterKeys.METIS_DATASET_ID, EXAMPLE_METIS_DATASET_ID);

    String injectXmlFileName = "/xmlForTestingParamInjection.xml";
    CommonTaskTuple tuple = new CommonTaskTuple(
            new TaskData(TASK_ID, TASK_NAME),
            new RecordData(SOURCE_VERSION_URL, readMockContentOfURL(injectXmlFileName), true),
            new ProcessingData());
    tuple.setParameters(parameters);
    tuple.setOutputRevision(new Revision(REVISION_NAME, REVISION_PROVIDER, new Date()));
    xsltBolt.execute(anchorTuple, tuple);
    when(outputCollector.emit(any(Tuple.class), anyList())).thenReturn(null);
    verify(outputCollector, times(1)).emit(Mockito.any(Tuple.class), captor.capture());
    assertThat(captor.getAllValues().size(), is(1));
    List<Values> allValues = captor.getAllValues();
    assertEmittedTuple(allValues, 4);

    String transformed = new String(((RecordData) allValues.get(0).get(5)).getFileData());
    assertNotNull(transformed);
    assertTrue(transformed.contains(EXAMPLE_METIS_DATASET_ID));
  }

  @Test
  void shouldTransformNonEdmFile() throws IOException {
    //
    wireMockExtension.stubFor(get(urlEqualTo("/file"))
            .willReturn(aResponse()
                    .withStatus(200)
                    .withFixedDelay(150)
                    .withBodyFile("toEdmTransformer.xsl")));
    //
    Tuple anchorTuple = mock(TupleImpl.class);
    String sampleXmlFileName = "/nonEdmFile.xml";
    CommonTaskTuple tuple = new CommonTaskTuple(
            new TaskData(TASK_ID, TASK_NAME),
            new RecordData(SOURCE_VERSION_URL, readMockContentOfURL(sampleXmlFileName), true),
            new ProcessingData());
    tuple.setParameters(prepareStormTaskTupleParametersForNonEDMProcessing());
    tuple.setOutputRevision(new Revision(REVISION_NAME, REVISION_PROVIDER, new Date()));
    xsltBolt.execute(anchorTuple, tuple);
    when(outputCollector.emit(any(Tuple.class), anyList())).thenReturn(null);
    verify(outputCollector, times(1)).emit(Mockito.any(Tuple.class), captor.capture());
    assertThat(captor.getAllValues().size(), is(1));
    List<Values> allValues = captor.getAllValues();
    assertEmittedTuple(allValues, 4);

    String transformed = new String(((RecordData) allValues.get(0).get(5)).getFileData());

    assertNotNull(transformed);
    assertTrue(transformed.contains(EXAMPLE_METIS_DATASET_ID));
    assertThat(transformed, hasXPath("/rdf:RDF").withNamespaceContext(NS));
    assertThat(transformed, hasXPath("/rdf:RDF/edm:ProvidedCHO").withNamespaceContext(NS));
    assertThat(transformed, hasXPath("/rdf:RDF/edm:ProvidedCHO/dc:title").withNamespaceContext(NS));
    assertThat(transformed, hasXPath("/rdf:RDF/edm:ProvidedCHO/dc:creator").withNamespaceContext(NS));
    assertThat(transformed, hasXPath("/rdf:RDF/edm:ProvidedCHO/dc:type").withNamespaceContext(NS));
    assertThat(transformed, hasXPath("/rdf:RDF/ore:Aggregation").withNamespaceContext(NS));
    assertThat(transformed, hasXPath("/rdf:RDF/edm:WebResource").withNamespaceContext(NS));
  }

  private HashMap<String, String> prepareStormTaskTupleParameters() {
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(PluginParameterKeys.XSLT_URL, "https://metis-core-rest.test.eanadev.org/datasets/xslt/default");
    parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
    return parameters;
  }

  private HashMap<String, String> prepareStormTaskTupleParametersForNonEDMProcessing() {
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(PluginParameterKeys.XSLT_URL, "http://127.0.0.1:9876/file");
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
    assertTrue(allValues.get(0).get(3) instanceof TaskData);
    var parameters = ((TaskData) allValues.get(0).get(3)).getParameters();
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