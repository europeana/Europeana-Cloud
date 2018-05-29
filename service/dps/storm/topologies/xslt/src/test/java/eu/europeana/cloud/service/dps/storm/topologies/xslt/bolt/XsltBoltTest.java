package eu.europeana.cloud.service.dps.storm.topologies.xslt.bolt;

import com.google.common.base.Charsets;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.apache.commons.io.IOUtils;
import org.apache.storm.shade.com.google.common.io.Resources;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static eu.europeana.cloud.service.dps.test.TestConstants.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.*;

/**
 * Created by Tarek on 7/18/2017.
 */
public class XsltBoltTest {
    public static final String EXAMPLE_METIS_DATASET_ID = "metis_dataset_id";
    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";

    private final String sampleXmlFileName = "/xmlForTesting.xml";
    private final String sampleXsltFileName = "sample_xslt.xslt";

    private final String injectXmlFileName = "/xmlForTestingParamInjection.xml";
    private final String injectNodeXsltFileName = "inject_node.xslt";

    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;


    @InjectMocks
    private XsltBolt xsltBolt = new XsltBolt();

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
        xsltBolt.prepare();
    }


    @Test
    public void executeBolt() throws IOException {
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, readMockContentOfURL(sampleXmlFileName), prepareStormTaskTupleParameters(sampleXsltFileName), new Revision());
        xsltBolt.execute(tuple);
        when(outputCollector.emit(anyList())).thenReturn(null);
        verify(outputCollector, times(1)).emit(captor.capture());
        assertThat(captor.getAllValues().size(), is(1));
        List<Values> allValues = captor.getAllValues();
        assertEmittedTuple(allValues, 4);
    }

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);


    private HashMap<String, String> prepareStormTaskTupleParameters(String xsltFile) throws MalformedURLException {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, "AUTHORIZATION_HEADER");
        URL xsltFileUrl = Resources.getResource(xsltFile);
        parameters.put(PluginParameterKeys.XSLT_URL, xsltFileUrl.toString());
        return parameters;
    }


    private byte[] readFile(String fileName) throws IOException {
        String myXml = IOUtils.toString(getClass().getResource(fileName),
                Charsets.UTF_8);
        byte[] bytes = myXml.getBytes("UTF-8");
        InputStream contentStream = new ByteArrayInputStream(bytes);
        return IOUtils.toByteArray(contentStream);

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
        String authorizationHeader = parameters.get(PluginParameterKeys.AUTHORIZATION_HEADER);
        assertNotNull(authorizationHeader);


    }

    @Test
    public void executeBoltWithInjection() throws IOException {
        HashMap<String, String> parameters = prepareStormTaskTupleParameters(injectNodeXsltFileName);
        parameters.put(PluginParameterKeys.METIS_DATASET_ID, EXAMPLE_METIS_DATASET_ID);

        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, readMockContentOfURL(injectXmlFileName), parameters, new Revision());
        xsltBolt.execute(tuple);
        when(outputCollector.emit(anyList())).thenReturn(null);
        verify(outputCollector, times(1)).emit(captor.capture());
        assertThat(captor.getAllValues().size(), is(1));
        List<Values> allValues = captor.getAllValues();
        assertEmittedTuple(allValues, 5);

        String transformed = new String((byte[]) allValues.get(0).get(3));
        assertNotNull(transformed);
        assertTrue(transformed.contains(EXAMPLE_METIS_DATASET_ID));
    }
}