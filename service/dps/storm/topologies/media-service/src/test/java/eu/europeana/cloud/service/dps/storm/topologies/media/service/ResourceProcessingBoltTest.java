package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.metis.mediaprocessing.MediaExtractor;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.mediaprocessing.model.*;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by Tarek on 12/11/2018.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "org.apache.logging.log4j.*", "javax.xml.*", "org.xml.sax.*", "org.w3c.dom.*"})
public class ResourceProcessingBoltTest {

    private final static String AWS_ACCESS_KEY = "AWS_ACCESS_KEY";
    private final static String AWS_SECRET_KEY = "AWS_SECRET_KEY";
    private final static String AWS_END_POINT = "AWS_END_POINT";
    private final static String AWS_BUCKET = "AWS_BUCKET";
    private static final String MEDIA_RESOURCE_EXCEPTION = "media resource exception";

    public static final String FILE_URL = "FILE_URL";
    private static final String AUTHORIZATION = "Authorization";


    private StormTaskTuple stormTaskTuple;


    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @Mock(name = "amazonClient")
    private AmazonS3 amazonClient;


    @Mock(name = "mediaExtractor")
    private MediaExtractor mediaExtractor;

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);


    @InjectMocks
    static ResourceProcessingBolt resourceProcessingBolt = new ResourceProcessingBolt(AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_END_POINT, AWS_BUCKET);


    @Before
    public void prepareTuple() {
        PowerMockito.mockStatic(ResourceProcessingBolt.class);
        ResourceProcessingBolt.amazonClient = amazonClient;
        resourceProcessingBolt.initGson();
        stormTaskTuple = new StormTaskTuple();
        stormTaskTuple.setFileUrl(FILE_URL);
        stormTaskTuple.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, FILE_URL);
        stormTaskTuple.addParameter(PluginParameterKeys.AUTHORIZATION_HEADER, AUTHORIZATION);
    }


    @Test
    public void shouldSuccessfullyProcessTheResource() throws Exception {
        stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, Integer.toString(5));
        stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINK_KEY, "{\"resourceUrl\":\"http://contribute.europeana.eu/media/d2136d50-5b4c-0136-9258-16256f71c4b1\",\"urlTypes\":[\"HAS_VIEW\"]}");

        String resourceName = "RESOURCE_URL";
        int thumbnailCount = 3;
        List<Thumbnail> thumbnailList = getThumbnails(resourceName, thumbnailCount);

        AbstractResourceMetadata resourceMetadata = new TextResourceMetadata("text/xml", resourceName, 100, false, 10, thumbnailList);
        ResourceExtractionResult resourceExtractionResult = new ResourceExtractionResult(resourceMetadata, thumbnailList);

        when(mediaExtractor.performMediaExtraction(any(RdfResourceEntry.class))).thenReturn(resourceExtractionResult);
        when(amazonClient.putObject(eq(AWS_BUCKET), anyString(), any(InputStream.class), isNull(ObjectMetadata.class))).thenReturn(new PutObjectResult());
        resourceProcessingBolt.execute(stormTaskTuple);

        verify(amazonClient, Mockito.times(3)).putObject(eq(AWS_BUCKET), anyString(), any(InputStream.class), isNull(ObjectMetadata.class));
        verify(outputCollector, Mockito.times(1)).emit(captor.capture());
        Values value = captor.getValue();
        Map<String, String> parameters = (Map) value.get(4);
        assertNotNull(parameters);
        assertEquals(4, parameters.size());
        assertNull(parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE));
    }


    @Test
    public void shouldFormulateTheAggregateExceptionsWhenSavingToAmazonFails() throws Exception {
        stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, Integer.toString(5));
        stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINK_KEY, "{\"resourceUrl\":\"http://contribute.europeana.eu/media/d2136d50-5b4c-0136-9258-16256f71c4b1\",\"urlTypes\":[\"HAS_VIEW\"]}");

        String resourceName = "RESOURCE_URL";
        int thumbNailCount = 3;
        List<Thumbnail> thumbnailList = getThumbnails(resourceName, thumbNailCount);

        AbstractResourceMetadata resourceMetadata = new TextResourceMetadata("text/xml", resourceName, 100, false, 10, thumbnailList);
        ResourceExtractionResult resourceExtractionResult = new ResourceExtractionResult(resourceMetadata, thumbnailList);
        String errorMessage = "The error was thrown because of something";

        when(mediaExtractor.performMediaExtraction(any(RdfResourceEntry.class))).thenReturn(resourceExtractionResult);
        doThrow(new AmazonServiceException(errorMessage)).when(amazonClient).putObject(eq(AWS_BUCKET), anyString(), any(InputStream.class), isNull(ObjectMetadata.class));
        resourceProcessingBolt.execute(stormTaskTuple);

        verify(amazonClient, Mockito.times(3)).putObject(eq(AWS_BUCKET), anyString(), any(InputStream.class), isNull(ObjectMetadata.class));
        verify(outputCollector, Mockito.times(1)).emit(captor.capture());
        Values value = captor.getValue();
        Map<String, String> parameters = (Map) value.get(4);
        assertNotNull(parameters);
        assertEquals(6, parameters.size());
        assertNotNull(parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE));
        assertNotNull(parameters.get(PluginParameterKeys.UNIFIED_ERROR_MESSAGE));
        assertEquals(thumbNailCount, StringUtils.countMatches(parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE), errorMessage));
        assertNull(parameters.get(PluginParameterKeys.RESOURCE_LINK_KEY));
        assertNotNull(MEDIA_RESOURCE_EXCEPTION, parameters.get(PluginParameterKeys.UNIFIED_ERROR_MESSAGE));

    }


    @Test
    public void shouldSendExceptionsWhenProcessingFails() throws Exception {
        stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, Integer.toString(5));
        stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINK_KEY, "{\"resourceUrl\":\"http://contribute.europeana.eu/media/d2136d50-5b4c-0136-9258-16256f71c4b1\",\"urlTypes\":[\"HAS_VIEW\"]}");
        doThrow(MediaExtractionException.class).when(mediaExtractor).performMediaExtraction(any(RdfResourceEntry.class));

        resourceProcessingBolt.execute(stormTaskTuple);

        verify(outputCollector, Mockito.times(1)).emit(captor.capture());
        verify(amazonClient, Mockito.times(0)).putObject(eq(AWS_BUCKET), anyString(), any(InputStream.class), isNull(ObjectMetadata.class));

        Values value = captor.getValue();
        Map<String, String> parameters = (Map) value.get(4);
        assertNotNull(parameters);
        assertEquals(5, parameters.size());
        assertNotNull(parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE));
        assertNotNull(parameters.get(PluginParameterKeys.UNIFIED_ERROR_MESSAGE));
        assertNull(parameters.get(PluginParameterKeys.RESOURCE_METADATA));
        assertNull(parameters.get(PluginParameterKeys.RESOURCE_LINK_KEY));
        assertNotNull(MEDIA_RESOURCE_EXCEPTION, parameters.get(PluginParameterKeys.UNIFIED_ERROR_MESSAGE));

    }


    @Test
    public void shouldForwardTheTupleWhenNoResourceLinkFound() throws Exception {
        resourceProcessingBolt.execute(stormTaskTuple);
        int expectedParametersSize = 2;
        assertEquals(expectedParametersSize, stormTaskTuple.getParameters().size());
        verify(outputCollector, Mockito.times(1)).emit(captor.capture());
        Values value = captor.getValue();
        Map<String, String> parameters = (Map) value.get(4);
        assertNotNull(parameters);
        assertEquals(expectedParametersSize, parameters.size());
        assertNull(parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE));
        assertNull(parameters.get(PluginParameterKeys.RESOURCE_METADATA));
    }

    private List<Thumbnail> getThumbnails(String resourceName, int thumbnailCount) throws IOException {
        List<Thumbnail> list = new ArrayList<>();
        for (int i = 0; i < thumbnailCount; i++) {
            String thumbnailName = "TargetName" + i;
            Thumbnail thumbnail = new ThumbnailImpl(resourceName, thumbnailName);
            list.add(thumbnail);
        }
        return list;

    }
}