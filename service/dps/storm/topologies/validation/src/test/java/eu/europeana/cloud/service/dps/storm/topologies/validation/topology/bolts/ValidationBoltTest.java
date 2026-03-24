package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts;

import com.github.tomakehurst.wiremock.junit5.WireMockExtension;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.tuple.common.CommonTaskTuple;
import eu.europeana.cloud.service.dps.storm.tuple.common.RecordMetadata;
import eu.europeana.cloud.service.dps.storm.tuple.common.StormProcessingMetadata;
import eu.europeana.cloud.service.dps.storm.tuple.common.TaskMetadata;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.Properties;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static org.mockito.Mockito.mock;

@ExtendWith(WireMockExtension.class)
class ValidationBoltTest {

  @Mock(name = "outputCollector")
  private OutputCollector outputCollector;

  private final int TASK_ID = 1;
  private final String TASK_NAME = "TASK_NAME";


  @RegisterExtension
  static WireMockExtension wireMockExtension =
          WireMockExtension.newInstance()
                  .options(wireMockConfig().dynamicPort())
                  .build();

  @InjectMocks
  private ValidationBolt validationBolt;

  @BeforeEach
  void init() {
    validationBolt = new ValidationBolt(new CassandraProperties(), readProperties());
    // For some reason test crashes without this even though we use MockitoExtension?
    MockitoAnnotations.initMocks(this);

    wireMockExtension.resetAll();
    wireMockExtension.stubFor(get(urlEqualTo("/test_schema.zip"))
            .willReturn(aResponse()
                    .withStatus(200)
                    .withFixedDelay(2000)
                    .withBodyFile("test_schema.zip")));
    wireMockExtension.stubFor(get(urlEqualTo("/edm_sorter.xsl"))
            .willReturn(aResponse()
                    .withStatus(200)
                    .withFixedDelay(10)
                    .withBodyFile("edm_sorter.xsl")));
    validationBolt.prepare();
  }


  @ParameterizedTest
  @CsvSource({
          "src/test/resources/Item_35834473_test.xml, edm-internal, null", //validateEdmInternalFile
          "src/test/resources/Item_35834473_test.xml, edm-internal, EDM-INTERNAL.xsd",
          //validateEdmInternalFileWithProvidedRootLocation
          "src/test/resources/Item_35834473.xml, edm-external, null", //validateEdmExternalFile
          "src/test/resources/edmExternalWithOutOfOrderElements.xml, edm-external, null" //validateEdmExternalOutOfOrderFile
  })
  void validateEdm(String resourcePath, String schemaName, String schemaRootLocation) throws IOException {
    Tuple anchorTuple = mock(TupleImpl.class);
    byte[] FILE_DATA = Files.readAllBytes(Paths.get(resourcePath));
    CommonTaskTuple tuple = new CommonTaskTuple(
            new TaskMetadata(TASK_ID, TASK_NAME),
            new RecordMetadata(SOURCE_VERSION_URL, FILE_DATA, true),
            new StormProcessingMetadata());
    tuple.setParameters(prepareStormTaskTupleParameters(schemaName, schemaRootLocation));
    tuple.setOutputRevision(new Revision(REVISION_NAME, REVISION_PROVIDER, new Date()));
    validationBolt.execute(anchorTuple, tuple);
    assertSuccessfulValidation();
  }

  @Test
  void sendErrorNotificationWhenTheValidationFails() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/Item_35834473_test.xml"));
    CommonTaskTuple tuple = new CommonTaskTuple(
            new TaskMetadata(TASK_ID, TASK_NAME),
            new RecordMetadata(SOURCE_VERSION_URL, FILE_DATA, true),
            new StormProcessingMetadata());
    tuple.setParameters(prepareStormTaskTupleParameters("edm-external", null));
    tuple.setOutputRevision(new Revision(REVISION_NAME, REVISION_PROVIDER, new Date()));
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
    props.put("edmSorterFileLocation", "http://127.0.0.1:" + wireMockExtension.getPort() + "/edm_sorter.xsl");
    props.put("predefinedSchemas.edm-internal.url", "http://127.0.0.1:" + wireMockExtension.getPort() + "/test_schema.zip");
    props.put("predefinedSchemas.edm-internal.rootLocation", "EDM-INTERNAL.xsd");
    props.put("predefinedSchemas.edm-external.url", "http://127.0.0.1:" + wireMockExtension.getPort() + "/test_schema.zip");
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