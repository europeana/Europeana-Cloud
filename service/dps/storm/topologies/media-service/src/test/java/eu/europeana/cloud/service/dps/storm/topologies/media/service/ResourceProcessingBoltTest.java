package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.metis.mediaprocessing.MediaExtractor;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.mediaprocessing.model.*;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Matchers.nullable;
import static org.mockito.Mockito.*;

/**
 * Created by Tarek on 12/11/2018.
 */
public class ResourceProcessingBoltTest {

    private final static String AWS_ACCESS_KEY = "AWS_ACCESS_KEY";
    private final static String AWS_SECRET_KEY = "AWS_SECRET_KEY";
    private final static String AWS_END_POINT = "AWS_END_POINT";
    private final static String AWS_BUCKET = "AWS_BUCKET";
    private static final String MEDIA_RESOURCE_EXCEPTION = "media resource exception";
    private static final long TASK_ID = 1;

    public static final String FILE_URL = "FILE_URL";
    private static final String AUTHORIZATION = "Authorization";


    private StormTaskTuple stormTaskTuple;


    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @Mock
    private AmazonClient amazonClient;

    @Mock(name = "mediaExtractor")
    private MediaExtractor mediaExtractor;

    @Mock(name = "taskStatusChecker")
    private TaskStatusChecker taskStatusChecker;

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);


    @InjectMocks
    ResourceProcessingBolt resourceProcessingBolt = new ResourceProcessingBolt(amazonClient);


    @Before
    public void prepareTuple() throws Exception {
        MockitoAnnotations.initMocks(this);
        resourceProcessingBolt.initGson();
        stormTaskTuple = new StormTaskTuple();
        stormTaskTuple.setFileUrl(FILE_URL);
        stormTaskTuple.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, FILE_URL);
        stormTaskTuple.addParameter(PluginParameterKeys.AUTHORIZATION_HEADER, AUTHORIZATION);
        stormTaskTuple.setTaskId(TASK_ID);

        setStaticField(ResourceProcessingBolt.class.getSuperclass().getDeclaredField("taskStatusChecker"), taskStatusChecker);
    }

    void setStaticField(Field field, Object newValue) throws Exception {
        field.setAccessible(true);
        field.set(null, newValue);
    }


    @Test
    public void shouldSuccessfullyProcessTheResource() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, Integer.toString(5));
        stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINK_KEY, "{\"resourceUrl\":\"http://contribute.europeana.eu/media/d2136d50-5b4c-0136-9258-16256f71c4b1\",\"urlTypes\":[\"HAS_VIEW\"]}");

        String resourceName = "RESOURCE_URL";
        int thumbnailCount = 3;
        List<Thumbnail> thumbnailList = getThumbnails(thumbnailCount);

        AbstractResourceMetadata resourceMetadata = new TextResourceMetadata("text/xml", resourceName, 100L, false, 10, thumbnailList);
        ResourceExtractionResult resourceExtractionResult = new ResourceExtractionResultImpl(resourceMetadata, thumbnailList);

        when(mediaExtractor.performMediaExtraction(any(RdfResourceEntry.class), anyBoolean())).thenReturn(resourceExtractionResult);
        when(amazonClient.putObject(anyString(), any(InputStream.class), nullable(ObjectMetadata.class))).thenReturn(new PutObjectResult());
        when(taskStatusChecker.hasKillFlag(eq(TASK_ID))).thenReturn(false);
        resourceProcessingBolt.execute(anchorTuple, stormTaskTuple);

        verify(amazonClient, Mockito.times(thumbnailCount)).putObject(anyString(), any(InputStream.class), any(ObjectMetadata.class));
        verify(outputCollector, Mockito.times(1)).emit(eq(anchorTuple), captor.capture());
        Values value = captor.getValue();
        Map<String, String> parameters = (Map) value.get(4);
        assertNotNull(parameters);
        assertEquals(4, parameters.size());
        assertNull(parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE));
    }


    @Test
    public void shouldDropTheTaskAndStopProcessing() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, Integer.toString(5));
        stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINK_KEY, "{\"resourceUrl\":\"http://contribute.europeana.eu/media/d2136d50-5b4c-0136-9258-16256f71c4b1\",\"urlTypes\":[\"HAS_VIEW\"]}");

        String resourceName = "RESOURCE_URL";
        int thumbnailCount = 3;
        List<Thumbnail> thumbnailList = getThumbnails(thumbnailCount);

        AbstractResourceMetadata resourceMetadata = new TextResourceMetadata("text/xml", resourceName, 100L, false, 10, thumbnailList);
        ResourceExtractionResult resourceExtractionResult = new ResourceExtractionResultImpl(resourceMetadata, thumbnailList);

        when(mediaExtractor.performMediaExtraction(any(RdfResourceEntry.class), anyBoolean())).thenReturn(resourceExtractionResult);
        when(amazonClient.putObject(anyString(), any(InputStream.class), isNull(ObjectMetadata.class))).thenReturn(new PutObjectResult());

        when(taskStatusChecker.hasKillFlag(eq(TASK_ID))).thenReturn(false).thenReturn(true);

        resourceProcessingBolt.execute(anchorTuple, stormTaskTuple);
        verify(amazonClient, Mockito.times(1)).putObject(anyString(), any(InputStream.class), any(ObjectMetadata.class));
    }


    @Test
    public void shouldFormulateTheAggregateExceptionsWhenSavingToAmazonFails() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, Integer.toString(5));
        stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINK_KEY, "{\"resourceUrl\":\"http://contribute.europeana.eu/media/d2136d50-5b4c-0136-9258-16256f71c4b1\",\"urlTypes\":[\"HAS_VIEW\"]}");

        String resourceName = "RESOURCE_URL";
        int thumbNailCount = 3;
        List<Thumbnail> thumbnailList = getThumbnails(thumbNailCount);

        AbstractResourceMetadata resourceMetadata = new TextResourceMetadata("text/xml", resourceName, 100L, false, 10, thumbnailList);
        ResourceExtractionResult resourceExtractionResult = new ResourceExtractionResultImpl(resourceMetadata, thumbnailList);
        String errorMessage = "The error was thrown because of something";

        when(mediaExtractor.performMediaExtraction(any(RdfResourceEntry.class), anyBoolean())).thenReturn(resourceExtractionResult);
        doThrow(new AmazonServiceException(errorMessage)).when(amazonClient).putObject( anyString(), any(InputStream.class), any(ObjectMetadata.class));
        resourceProcessingBolt.execute(anchorTuple, stormTaskTuple);

        verify(amazonClient, Mockito.times(3)).putObject(anyString(), any(InputStream.class), any(ObjectMetadata.class));
        verify(outputCollector, Mockito.times(1)).emit(eq(anchorTuple), captor.capture());
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
        Tuple anchorTuple = mock(TupleImpl.class);
        stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, Integer.toString(5));
        stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINK_KEY, "{\"resourceUrl\":\"http://contribute.europeana.eu/media/d2136d50-5b4c-0136-9258-16256f71c4b1\",\"urlTypes\":[\"HAS_VIEW\"]}");
        doThrow(MediaExtractionException.class).when(mediaExtractor).performMediaExtraction(any(RdfResourceEntry.class), anyBoolean());

        resourceProcessingBolt.execute(anchorTuple, stormTaskTuple);

        verify(outputCollector, Mockito.times(1)).emit(eq(anchorTuple), captor.capture());
        verify(amazonClient, Mockito.times(0)).putObject(anyString(), any(InputStream.class), isNull(ObjectMetadata.class));

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
        Tuple anchorTuple = mock(TupleImpl.class);
        resourceProcessingBolt.execute(anchorTuple, stormTaskTuple);
        int expectedParametersSize = 2;
        assertEquals(expectedParametersSize, stormTaskTuple.getParameters().size());
        verify(outputCollector, Mockito.times(1)).emit(eq(anchorTuple), captor.capture());
        Values value = captor.getValue();
        Map<String, String> parameters = (Map) value.get(4);
        assertNotNull(parameters);
        assertEquals(expectedParametersSize, parameters.size());
        assertNull(parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE));
        assertNull(parameters.get(PluginParameterKeys.RESOURCE_METADATA));
    }

    private List<Thumbnail> getThumbnails(int thumbnailCount) throws IOException {
        List<Thumbnail> list = new ArrayList<>();
        for (int i = 0; i < thumbnailCount; i++) {
            Thumbnail thumbnail = mock(Thumbnail.class);
            String thumbnailName = "TargetName" + i;
            when(thumbnail.getContentSize()).thenReturn(1l);
            when(thumbnail.getTargetName()).thenReturn(thumbnailName);
            when(thumbnail.getContentStream()).thenReturn(Mockito.mock(InputStream.class));
            list.add(thumbnail);
        }
        return list;

    }
}