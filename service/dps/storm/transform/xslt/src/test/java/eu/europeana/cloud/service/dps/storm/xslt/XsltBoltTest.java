package eu.europeana.cloud.service.dps.storm.xslt;

import com.google.common.base.Charsets;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.apache.commons.io.IOUtils;
import org.apache.storm.shade.com.google.common.io.Resources;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.*;

/**
 * Created by Tarek on 7/18/2017.
 */
public class XsltBoltTest {
    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";

    private final String sampleXmlFileName = "/xmlForTesting.xml";
    private final String sampleXsltFileName = "sample_xslt.xslt";


    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;


    @InjectMocks
    private XsltBolt xsltBolt = new XsltBolt();

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }


    @Test
    public void executeBolt() throws IOException {
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, SOURCE_VERSION_URL, readMockContentOfURL(), prepareStormTaskTupleParameters(), new Revision());
        xsltBolt.execute(tuple);
        when(outputCollector.emit(any(Tuple.class), anyList())).thenReturn(null);
        verify(outputCollector, times(1)).emit(any(Tuple.class), captor.capture());
        assertThat(captor.getAllValues().size(), is(1));
        List<Values> allValues = captor.getAllValues();
        assertEmittedTuple(allValues);
    }

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);


    private HashMap<String, String> prepareStormTaskTupleParameters() throws MalformedURLException {
        HashMap<String, String> parameters = new HashMap<>();
        parameters.put(PluginParameterKeys.AUTHORIZATION_HEADER, "AUTHORIZATION_HEADER");
        URL xsltFileUrl = Resources.getResource(sampleXsltFileName);
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
    private byte[] readMockContentOfURL() throws IOException {
        return readFile(sampleXmlFileName);
    }

    private void assertEmittedTuple(List<Values> allValues) {
        assertNotNull(allValues);
        assertEquals(allValues.size(), 1);

        //parameters assertion
        assertTrue(allValues.get(0).get(4) instanceof Map);
        Map<String, String> parameters = ((Map<String, String>) allValues.get(0).get(4));
        assertNotNull(parameters);
        assertEquals(parameters.size(), 4);
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
}