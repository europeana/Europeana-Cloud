package eu.europeana.cloud.http.bolts;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class HttpHarvestingBoltTest {

  private static final String TASK_NAME = "TASK_NAME";
  private static final long TASK_ID = -5964014235733572511L;
  private static final String TASK_RELATIVE_URL = "/http_harvest/task_-5964014235733572511/";
  private String fileUrl;

  private final Optional<Integer> optOverriddenRetryAttemptsCount = Optional.ofNullable(
      RetryableMethodExecutor.OVERRIDE_ATTEMPT_COUNT);
  private StormTaskTuple tuple;

  @InjectMocks
  private HttpHarvestingBolt bolt = new HttpHarvestingBolt(new CassandraProperties());

  @Mock
  private Tuple anchorTuple;

  @Mock
  private OutputCollector outputCollector;

  @Captor
  private ArgumentCaptor<List<Object>> resultTupleCaptor;

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

  @Before
  public void setup() throws IllegalAccessException {
    wireMockRule.resetAll();
    fileUrl = "http://localhost:" + wireMockRule.port() + "/http_harvest/task_-5964014235733572511/record.xml";
    tuple = new StormTaskTuple(TASK_ID, TASK_NAME, fileUrl, null, prepareStormTaskTupleParameters(), new Revision());
    bolt.prepare();
  }

  @Test
  public void shouldHarvestEdmFile() throws IOException {
    mockFileOnHttpServer("record.xml");

    bolt.execute(anchorTuple, tuple);

    verify(outputCollector).emit(eq(anchorTuple), resultTupleCaptor.capture());
    StormTaskTuple resultTuple = getResultStormTaskTuple();
    assertArrayEquals(readTestFile("record.xml"), resultTuple.getFileData());
    assertEquals("/100/object_DCU_24927017", resultTuple.getParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER));
    assertEquals("http://more.locloud.eu/object/DCU/24927017",
        resultTuple.getParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER));
    //Allow two possible values cause detected MIME type is OS (and even distribution) dependent.
    assertThat(resultTuple.getParameter(PluginParameterKeys.OUTPUT_MIME_TYPE),
        anyOf(is(MediaType.TEXT_XML), is(MediaType.APPLICATION_XML)));
  }

  @Test
  public void shouldRetryWhenCantDownloadFileFirstTime() throws IOException {
    assumeTrue((
        optOverriddenRetryAttemptsCount.isPresent() && optOverriddenRetryAttemptsCount.get() > 1)
        || optOverriddenRetryAttemptsCount.isEmpty());
    mockErrorOnHttpOnFirstTryServer("record.xml");

    bolt.execute(anchorTuple, tuple);

    verify(outputCollector).emit(eq(anchorTuple), resultTupleCaptor.capture());
    StormTaskTuple resultTuple = getResultStormTaskTuple();
    assertArrayEquals(readTestFile("record.xml"), resultTuple.getFileData());
    assertEquals("/100/object_DCU_24927017", resultTuple.getParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER));
    assertEquals("http://more.locloud.eu/object/DCU/24927017",
        resultTuple.getParameter(PluginParameterKeys.ADDITIONAL_LOCAL_IDENTIFIER));
    //Allow two possible values cause detected MIME type is OS (and even distribution) dependent.
    assertThat(resultTuple.getParameter(PluginParameterKeys.OUTPUT_MIME_TYPE),
        anyOf(is(MediaType.TEXT_XML), is(MediaType.APPLICATION_XML)));
  }

  @Test
  public void shouldEmitErrorNotificationWhenCantLoadFilePermanently() {
    mockErrorOnHttpOnServer("record.xml");

    bolt.execute(anchorTuple, tuple);

    verify(outputCollector, never()).emit(eq(anchorTuple), any());
    verify(outputCollector).emit(eq(NOTIFICATION_STREAM_NAME), eq(anchorTuple), resultTupleCaptor.capture());
  }

  private byte[] readTestFile(String fileName) throws IOException {
    return getClass().getResourceAsStream("/__files/" + fileName).readAllBytes();
  }

  private void mockFileOnHttpServer(String fileName) {
    wireMockRule.stubFor(get(urlEqualTo(fileRelativeUrl(fileName)))
        .willReturn(aResponse()
            .withStatus(200)
            .withFixedDelay(150)
            .withBodyFile(fileName)));
  }

  private void mockErrorOnHttpOnServer(String fileName) {
    wireMockRule.stubFor(get(urlEqualTo(fileRelativeUrl(fileName)))
        .willReturn(aResponse()
            .withStatus(500)
            .withFixedDelay(150)));

  }

  private void mockErrorOnHttpOnFirstTryServer(String fileName) {
    wireMockRule.stubFor(get(urlEqualTo(fileRelativeUrl(fileName)))
        .inScenario("Retry")
        .whenScenarioStateIs(STARTED)
        .willReturn(aResponse()
            .withStatus(500)
            .withFixedDelay(150))
        .willSetStateTo("retried"));

    wireMockRule.stubFor(get(urlEqualTo(fileRelativeUrl(fileName)))
        .inScenario("Retry")
        .whenScenarioStateIs("retried")
        .willReturn(aResponse()
            .withStatus(200)
            .withFixedDelay(150)
            .withBodyFile(fileName)));
  }

  private String fileRelativeUrl(String fileName) {
    return TASK_RELATIVE_URL + fileName;
  }

  private StormTaskTuple getResultStormTaskTuple() {
    return StormTaskTuple.fromValues(resultTupleCaptor.getValue());
  }

  private HashMap<String, String> prepareStormTaskTupleParameters() {
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
    parameters.put(PluginParameterKeys.METIS_DATASET_ID, "100");
    return parameters;
  }
}