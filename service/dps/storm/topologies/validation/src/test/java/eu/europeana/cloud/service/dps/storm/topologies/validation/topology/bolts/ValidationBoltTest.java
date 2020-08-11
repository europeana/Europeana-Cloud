package eu.europeana.cloud.service.dps.storm.topologies.validation.topology.bolts;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.properties.PropertyFileLoader;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static eu.europeana.cloud.service.dps.test.TestConstants.SOURCE_VERSION_URL;
import static org.mockito.Mockito.mock;

/**
 * Created by Tarek on 1/16/2018.
 */

public class ValidationBoltTest {


    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";


    @InjectMocks
    private ValidationBolt validationBolt = new ValidationBolt(readProperties("validation.properties"));

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(9999));

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);

        wireMockRule.resetAll();
        wireMockRule.stubFor(get(urlEqualTo("/test_schema.zip"))
                .willReturn(aResponse()
                        .withStatus(200)
                        .withFixedDelay(2000)
                        .withBodyFile("test_schema.zip")));
        validationBolt.prepare();
    }

    @Test
    public void validateEdmInternalFile() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/Item_35834473_test.xml"));
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParameters("edm-internal", null), new Revision());
        validationBolt.execute(anchorTuple, tuple);
        assertSuccessfulValidation();
    }

    @Test
    public void validateEdmInternalFileWithProvidedRootLocation() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/Item_35834473_test.xml"));
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParameters("edm-internal", "EDM-INTERNAL.xsd"), new Revision());
        validationBolt.execute(anchorTuple, tuple);
        assertSuccessfulValidation();
    }

    @Test
    public void validateEdmExternalFile() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/Item_35834473.xml"));
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParameters("edm-external", null), new Revision());
        validationBolt.execute(anchorTuple, tuple);
        assertSuccessfulValidation();
    }

    @Test
    public void validateEdmExternalOutOfOrderFile() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/edmExternalWithOutOfOrderElements.xml"));
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParameters("edm-external", null), new Revision());
        validationBolt.execute(anchorTuple, tuple);
        assertSuccessfulValidation();
    }

    @Test
    public void sendErrorNotificationWhenTheValidationFails() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        byte[] FILE_DATA = Files.readAllBytes(Paths.get("src/test/resources/Item_35834473_test.xml"));
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, FILE_DATA, prepareStormTaskTupleParameters("edm-external", null), new Revision());
        validationBolt.execute(anchorTuple, tuple);
        assertFailedValidation();
    }

    private void assertSuccessfulValidation() {
        Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.any(List.class));
        Mockito.verify(outputCollector, Mockito.times(0)).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(List.class));
    }

    private void assertFailedValidation() {
        Mockito.verify(outputCollector, Mockito.times(0)).emit(Mockito.any(Tuple.class), Mockito.any(List.class));
        Mockito.verify(outputCollector, Mockito.times(1)).emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), Mockito.any(Tuple.class), Mockito.any(List.class));
    }

    private static Properties readProperties(String propertyFilename) {
        Properties props = new Properties();
        PropertyFileLoader.loadPropertyFile(propertyFilename, "", props);
        return props;
    }

    private HashMap<String, String> prepareStormTaskTupleParameters(String schemaName, String schemaRootLocation) throws MalformedURLException {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, "AUTHORIZATION_HEADER");
        parameters.put(PluginParameterKeys.SCHEMA_NAME, schemaName);
        parameters.put(PluginParameterKeys.ROOT_LOCATION, schemaRootLocation);
        return parameters;
    }

}