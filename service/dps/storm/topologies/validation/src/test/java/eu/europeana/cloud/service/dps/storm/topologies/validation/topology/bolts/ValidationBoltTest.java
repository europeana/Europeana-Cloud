package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE_VERSION_URL;
import static org.mockito.Mockito.mock;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@RunWith(JUnitParamsRunner.class)
public class ValidationBoltTest {

  @Mock(name = "outputCollector")
  private OutputCollector outputCollector;

  private final int TASK_ID = 1;
  private final String TASK_NAME = "TASK_NAME";


  @Rule
  public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().dynamicPort());

  @InjectMocks
  private ValidationBolt validationBolt;

  @Before
  public void init() {
    validationBolt = new ValidationBolt(new CassandraProperties(), readProperties());
    MockitoAnnotations.initMocks(this);

    wireMockRule.resetAll();
    wireMockRule.stubFor(get(urlEqualTo("/test_schema.zip"))
        .willReturn(aResponse()
            .withStatus(200)
            .withFixedDelay(2000)
            .withBodyFile("test_schema.zip")));
    wireMockRule.stubFor(get(urlEqualTo("/edm_sorter.xsl"))
        .willReturn(aResponse()
            .withStatus(200)
            .withFixedDelay(10)
            .withBodyFile("edm_sorter.xsl")));
    validationBolt.prepare();
  }


  @Test
  @Parameters({
      "src/test/resources/Item_35834473_test.xml, edm-internal, null", //validateEdmInternalFile
      "src/test/resources/Item_35834473_test.xml, edm-internal, EDM-INTERNAL.xsd",
      //validateEdmInternalFileWithProvidedRootLocation
      "src/test/resources/Item_35834473.xml, edm-external, null", //validateEdmExternalFile
      "src/test/resources/edmExternalWithOutOfOrderElements.xml, edm-external, null" //validateEdmExternalOutOfOrderFile
  })
  public void validateEdm(String resourcePath, String schemaName, String schemaRootLocation) throws IOException {
    Tuple anchorTuple = mock(TupleImpl.class);
    byte[] FILE_DATA = Files.readAllBytes(Paths.get(resourcePath));
    StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA,
        prepareStormTaskTupleParameters(schemaName, schemaRootLocation), new Revision());
    validationBolt.execute(anchorTuple, tuple);
    assertSuccessfulValidation();
  }

  @Test
  public void sendErrorNotificationWhenTheValidationFails() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/Item_35834473_test.xml"));
    StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA,
        prepareStormTaskTupleParameters("edm-external", null), new Revision());
    validationBolt.execute(anchorTuple, tuple);
    assertFailedValidation();
  }

  private void assertSuccessfulValidation() {
    Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.any(Tuple.class), Mockito.anyList());
    Mockito.verify(outputCollector, Mockito.times(0))
           .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(Tuple.class), Mockito.anyList());
  }

  private void assertFailedValidation() {
    Mockito.verify(outputCollector, Mockito.times(0)).emit(Mockito.any(Tuple.class), Mockito.anyList());
    Mockito.verify(outputCollector, Mockito.times(1))
           .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(Tuple.class), Mockito.anyList());
  }

  private Properties readProperties() {
    Properties props = new Properties();
    props.put("predefinedSchemas", "edm-internal,edm-external");
    props.put("edmSorterFileLocation", "http://127.0.0.1:" + wireMockRule.port() + "/edm_sorter.xsl");
    props.put("predefinedSchemas.edm-internal.url", "http://127.0.0.1:" + wireMockRule.port() + "/test_schema.zip");
    props.put("predefinedSchemas.edm-internal.rootLocation", "EDM-INTERNAL.xsd");
    props.put("predefinedSchemas.edm-external.url", "http://127.0.0.1:" + wireMockRule.port() + "/test_schema.zip");
    props.put("predefinedSchemas.edm-external.rootLocation", "EDM.xsd");
    return props;
  }

  private HashMap<String, String> prepareStormTaskTupleParameters(String schemaName, String schemaRootLocation) {
    HashMap<String, String> parameters = new HashMap<>();
    parameters.put(PluginParameterKeys.SCHEMA_NAME, schemaName);
    parameters.put(PluginParameterKeys.ROOT_LOCATION, schemaRootLocation);
    parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
    return parameters;
  }

}