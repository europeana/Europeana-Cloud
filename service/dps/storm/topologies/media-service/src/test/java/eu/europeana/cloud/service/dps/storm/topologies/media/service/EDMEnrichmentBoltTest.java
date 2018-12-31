package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.InputStream;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Created by Tarek on 12/14/2018.
 */
@RunWith(MockitoJUnitRunner.class)
public class EDMEnrichmentBoltTest {

    private static final String AUTHORIZATION = "Authorization";
    private static final String MEDIA_RESOURCE_EXCEPTION = "media resource exception";
    public static final String FILE_URL = "http://localhost:8080/mcs/records/sourceCloudId/representations/sourceRepresentationName/versions/sourceVersion/files/sourceFileName";

    private StormTaskTuple stormTaskTuple;

    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    @InjectMocks
    private static EDMEnrichmentBolt edmEnrichmentBolt = new EDMEnrichmentBolt();

    @BeforeClass
    public static void init() {
        edmEnrichmentBolt.prepare();
    }

    @Before
    public void initTuple() {
        MockitoAnnotations.initMocks(this);
        edmEnrichmentBolt.cache.clear();
        stormTaskTuple = new StormTaskTuple();
        stormTaskTuple.setFileUrl(FILE_URL);
        stormTaskTuple.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, FILE_URL);
        stormTaskTuple.addParameter(PluginParameterKeys.AUTHORIZATION_HEADER, AUTHORIZATION);
    }


    @Test
    public void shouldEnrichTheFileSuccessfullyAndSendItToTheNextBolt() throws Exception {
        try (InputStream stream = this.getClass().getResourceAsStream("/files/Item_35834473.xml")) {
            stormTaskTuple.setFileData(stream);
            stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_METADATA, "{\"textResourceMetadata\":{\"containsText\":false,\"resolution\":10,\"mimeType\":\"text/xml\",\"resourceUrl\":\"http://contribute.europeana.eu/media/d2136d50-5b4c-0136-9258-16256f71c4b1\",\"contentSize\":100,\"thumbnailTargetNames\":[\"TargetName1\",\"TargetName0\",\"TargetName2\"]}}");
            stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, String.valueOf(1));
            assertEquals(4, stormTaskTuple.getParameters().size());
            edmEnrichmentBolt.execute(stormTaskTuple);
            verify(outputCollector, times(1)).emit(captor.capture());
            Values values = captor.getValue();
            Map<String, String> parameters = (Map) values.get(4);
            assertNotNull(parameters);
            assertEquals(6, parameters.size());
            assertNull(parameters.get(PluginParameterKeys.RESOURCE_METADATA));
            assertEquals("sourceCloudId", parameters.get(PluginParameterKeys.CLOUD_ID));
            assertEquals("sourceRepresentationName", parameters.get(PluginParameterKeys.REPRESENTATION_NAME));
            assertEquals("sourceVersion", parameters.get(PluginParameterKeys.REPRESENTATION_VERSION));
        }
    }


    @Test
    public void shouldEnrichTheFileSuccessfullyOnMultipleBatchesAndSendItToTheNextBolt() throws Exception {
        try (InputStream stream = this.getClass().getResourceAsStream("/files/Item_35834473.xml")) {
            stormTaskTuple.setFileData(stream);
            stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_METADATA, "{\"textResourceMetadata\":{\"containsText\":false,\"resolution\":10,\"mimeType\":\"text/xml\",\"resourceUrl\":\"http://contribute.europeana.eu/media/d2136d50-5b4c-0136-9258-16256f71c4b1\",\"contentSize\":100,\"thumbnailTargetNames\":[\"TargetName1\",\"TargetName0\",\"TargetName2\"]}}");

            int resourceLinksCount = 10;
            stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, String.valueOf(resourceLinksCount));
            assertEquals(4, stormTaskTuple.getParameters().size());
            for (int i = 1; i <= resourceLinksCount; i++) {
                edmEnrichmentBolt.execute(stormTaskTuple);
                if (i < resourceLinksCount)
                    assertEquals(i, edmEnrichmentBolt.cache.get(FILE_URL).getCount());
            }
            verify(outputCollector, times(1)).emit(captor.capture());
            Values values = captor.getValue();
            Map<String, String> parameters = (Map) values.get(4);
            assertNotNull(parameters);
            assertEquals(6, parameters.size());
            assertNull(parameters.get(PluginParameterKeys.RESOURCE_METADATA));
            assertEquals("sourceCloudId", parameters.get(PluginParameterKeys.CLOUD_ID));
            assertEquals("sourceRepresentationName", parameters.get(PluginParameterKeys.REPRESENTATION_NAME));
            assertEquals("sourceVersion", parameters.get(PluginParameterKeys.REPRESENTATION_VERSION));
        }
    }


    @Test
    public void shouldForwardTheTupleWhenNoResourceLinkFound() throws Exception {
        edmEnrichmentBolt.execute(stormTaskTuple);
        int expectedParametersSize = 2;
        Map<String, String> initialTupleParameters = stormTaskTuple.getParameters();
        assertEquals(expectedParametersSize, initialTupleParameters.size());
        verify(outputCollector, Mockito.times(1)).emit(captor.capture());
        Values value = captor.getValue();
        Map<String, String> parametersAfterExecution = (Map) value.get(4);
        assertNotNull(parametersAfterExecution);
        assertEquals(expectedParametersSize, parametersAfterExecution.size());
        for (String key : parametersAfterExecution.keySet()) {
            assertTrue(initialTupleParameters.keySet().contains(key));
            assertEquals(initialTupleParameters.get(key), parametersAfterExecution.get(key));
        }
    }


    @Test
    public void shouldLogTheExceptionAndSendItAsParameterToTheNextBolt() throws Exception {
        try (InputStream stream = this.getClass().getResourceAsStream("/files/Item_35834473.xml")) {
            stormTaskTuple.setFileData(stream);
            String brokenMetaData = "{\"textResourceMetadata\":{\"containsTe/xml\",\"resourceUrl\":\"RESOURCE_URL\",\"contentSize\":100,\"thumbnailTargetNames\":[\"TargetName1\",\"TargetName0\",\"TargetName2\"]}}";
            stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_METADATA, brokenMetaData);
            stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, String.valueOf(1));
            assertEquals(4, stormTaskTuple.getParameters().size());
            edmEnrichmentBolt.execute(stormTaskTuple);
            verify(outputCollector, times(1)).emit(captor.capture());
            Values values = captor.getValue();
            Map<String, String> parameters = (Map) values.get(4);
            assertNotNull(parameters);
            assertEquals(8, parameters.size());
            assertNotNull(parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE));
            assertNotNull(parameters.get(PluginParameterKeys.UNIFIED_ERROR_MESSAGE));
            assertNull(parameters.get(PluginParameterKeys.RESOURCE_METADATA));
            assertEquals("sourceCloudId", parameters.get(PluginParameterKeys.CLOUD_ID));
            assertEquals("sourceRepresentationName", parameters.get(PluginParameterKeys.REPRESENTATION_NAME));
            assertEquals("sourceVersion", parameters.get(PluginParameterKeys.REPRESENTATION_VERSION));
            assertNotNull(parameters.get(PluginParameterKeys.UNIFIED_ERROR_MESSAGE));
            assertNotNull(MEDIA_RESOURCE_EXCEPTION, parameters.get(PluginParameterKeys.UNIFIED_ERROR_MESSAGE));

        }
    }


}