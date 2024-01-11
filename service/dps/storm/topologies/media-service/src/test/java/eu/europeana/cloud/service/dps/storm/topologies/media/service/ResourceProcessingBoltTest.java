package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.nullable;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.metis.mediaprocessing.MediaExtractor;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.mediaprocessing.model.AbstractResourceMetadata;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;
import eu.europeana.metis.mediaprocessing.model.ResourceExtractionResult;
import eu.europeana.metis.mediaprocessing.model.ResourceExtractionResultImpl;
import eu.europeana.metis.mediaprocessing.model.TextResourceMetadata;
import eu.europeana.metis.mediaprocessing.model.Thumbnail;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.util.reflection.FieldSetter;

public class ResourceProcessingBoltTest {

  private static final String MEDIA_RESOURCE_EXCEPTION = "media resource exception";
  private static final long TASK_ID = 1;

  public static final String FILE_URL = "FILE_URL";


  private StormTaskTuple stormTaskTuple;


  @Mock(name = "outputCollector")
  private OutputCollector outputCollector;

  @Mock
  private AmazonClient amazonClient;

  @Mock(name = "mediaExtractor")
  private MediaExtractor mediaExtractor;

  @Mock(name = "taskStatusChecker")
  private TaskStatusChecker taskStatusChecker;

  @InjectMocks
  private ThumbnailUploader thumbnailUploader;

  @Captor
  ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

  @InjectMocks
  ResourceProcessingBolt resourceProcessingBolt = new ResourceProcessingBolt(new CassandraProperties(), amazonClient);


  @Before
  public void prepareTuple() throws Exception {
    MockitoAnnotations.initMocks(this);
    FieldSetter.setField(resourceProcessingBolt,
        ResourceProcessingBolt.class.getDeclaredField("thumbnailUploader"), thumbnailUploader);
    resourceProcessingBolt.initGson();
    stormTaskTuple = new StormTaskTuple();
    stormTaskTuple.setFileUrl(FILE_URL);
    stormTaskTuple.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, FILE_URL);
    stormTaskTuple.setTaskId(TASK_ID);

    setStaticField(ResourceProcessingBolt.class.getSuperclass().getDeclaredField("taskStatusChecker"), taskStatusChecker);
  }

  void setStaticField(Field field, Object newValue) throws Exception {
    field.setAccessible(true);
    field.set(resourceProcessingBolt, newValue);
  }


  @Test
  public void shouldSuccessfullyProcessTheResource() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, Integer.toString(5));
    stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINK_KEY,
        "{\"resourceUrl\":\"http://contribute.europeana.eu/media/d2136d50-5b4c-0136-9258-16256f71c4b1\",\"urlTypes\":[\"HAS_VIEW\"]}");

    String resourceName = "RESOURCE_URL";
    int thumbnailCount = 3;
    List<Thumbnail> thumbnailList = getThumbnails(thumbnailCount);

    AbstractResourceMetadata resourceMetadata = new TextResourceMetadata("text/xml", resourceName, 100L, false, 10,
        thumbnailList);
    ResourceExtractionResult resourceExtractionResult = new ResourceExtractionResultImpl(resourceMetadata, thumbnailList);

    when(mediaExtractor.performMediaExtraction(any(RdfResourceEntry.class), anyBoolean())).thenReturn(resourceExtractionResult);
    when(amazonClient.putObject(anyString(), any(InputStream.class), nullable(ObjectMetadata.class))).thenReturn(
        new PutObjectResult());
    when(taskStatusChecker.hasDroppedStatus(TASK_ID)).thenReturn(false);
    resourceProcessingBolt.execute(anchorTuple, stormTaskTuple);

    verify(amazonClient, Mockito.times(thumbnailCount)).putObject(anyString(), any(InputStream.class), any(ObjectMetadata.class));
    verify(outputCollector, Mockito.times(1)).emit(eq(anchorTuple), captor.capture());
    Values value = captor.getValue();
    Map<String, String> parameters = (Map) value.get(4);
    assertNotNull(parameters);
    assertEquals(3, parameters.size());
    assertNull(parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE));
  }


  @Test
  public void shouldDropTheTaskAndStopProcessing() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, Integer.toString(5));
    stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINK_KEY,
        "{\"resourceUrl\":\"http://contribute.europeana.eu/media/d2136d50-5b4c-0136-9258-16256f71c4b1\",\"urlTypes\":[\"HAS_VIEW\"]}");

    String resourceName = "RESOURCE_URL";
    int thumbnailCount = 3;
    List<Thumbnail> thumbnailList = getThumbnails(thumbnailCount);

    AbstractResourceMetadata resourceMetadata = new TextResourceMetadata("text/xml", resourceName, 100L, false, 10,
        thumbnailList);
    ResourceExtractionResult resourceExtractionResult = new ResourceExtractionResultImpl(resourceMetadata, thumbnailList);

    when(mediaExtractor.performMediaExtraction(any(RdfResourceEntry.class), anyBoolean())).thenReturn(resourceExtractionResult);
    when(amazonClient.putObject(anyString(), any(InputStream.class), isNull(ObjectMetadata.class))).thenReturn(
        new PutObjectResult());

    when(taskStatusChecker.hasDroppedStatus(TASK_ID)).thenReturn(false).thenReturn(true);

    resourceProcessingBolt.execute(anchorTuple, stormTaskTuple);
    verify(amazonClient, Mockito.times(1)).putObject(anyString(), any(InputStream.class), any(ObjectMetadata.class));
  }


  @Test
  public void shouldFormulateTheAggregateExceptionsWhenSavingToAmazonFails() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, Integer.toString(5));
    stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINK_KEY,
        "{\"resourceUrl\":\"http://contribute.europeana.eu/media/d2136d50-5b4c-0136-9258-16256f71c4b1\",\"urlTypes\":[\"HAS_VIEW\"]}");

    String resourceName = "RESOURCE_URL";
    int thumbNailCount = 3;
    List<Thumbnail> thumbnailList = getThumbnails(thumbNailCount);

    AbstractResourceMetadata resourceMetadata = new TextResourceMetadata("text/xml", resourceName, 100L, false, 10,
        thumbnailList);
    ResourceExtractionResult resourceExtractionResult = new ResourceExtractionResultImpl(resourceMetadata, thumbnailList);
    String errorMessage = "The error was thrown because of something";

    when(mediaExtractor.performMediaExtraction(any(RdfResourceEntry.class), anyBoolean())).thenReturn(resourceExtractionResult);
    doThrow(new AmazonServiceException(errorMessage)).when(amazonClient)
                                                     .putObject(anyString(), any(InputStream.class), any(ObjectMetadata.class));
    resourceProcessingBolt.execute(anchorTuple, stormTaskTuple);

    verify(amazonClient, Mockito.times(3)).putObject(anyString(), any(InputStream.class), any(ObjectMetadata.class));
    verify(outputCollector, Mockito.times(1)).emit(eq(anchorTuple), captor.capture());
    Values value = captor.getValue();
    Map<String, String> parameters = (Map) value.get(4);
    assertNotNull(parameters);
    assertEquals(5, parameters.size());
    assertNotNull(parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE));
    assertNotNull(parameters.get(PluginParameterKeys.UNIFIED_ERROR_MESSAGE));
    assertEquals(thumbNailCount,
        StringUtils.countMatches(parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE), errorMessage));
    assertNull(parameters.get(PluginParameterKeys.RESOURCE_LINK_KEY));
    assertNotNull(MEDIA_RESOURCE_EXCEPTION, parameters.get(PluginParameterKeys.UNIFIED_ERROR_MESSAGE));

  }


  @Test
  public void shouldSendExceptionsWhenProcessingFails() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINKS_COUNT, Integer.toString(5));
    stormTaskTuple.addParameter(PluginParameterKeys.RESOURCE_LINK_KEY,
        "{\"resourceUrl\":\"http://contribute.europeana.eu/media/d2136d50-5b4c-0136-9258-16256f71c4b1\",\"urlTypes\":[\"HAS_VIEW\"]}");
    doThrow(MediaExtractionException.class).when(mediaExtractor)
                                           .performMediaExtraction(any(RdfResourceEntry.class), anyBoolean());

    resourceProcessingBolt.execute(anchorTuple, stormTaskTuple);

    verify(outputCollector, Mockito.times(1)).emit(eq(anchorTuple), captor.capture());
    verify(amazonClient, Mockito.times(0)).putObject(anyString(), any(InputStream.class), isNull(ObjectMetadata.class));

    Values value = captor.getValue();
    Map<String, String> parameters = (Map) value.get(4);
    assertNotNull(parameters);
    assertEquals(4, parameters.size());
    assertNotNull(parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE));
    assertNotNull(parameters.get(PluginParameterKeys.UNIFIED_ERROR_MESSAGE));
    assertNull(parameters.get(PluginParameterKeys.RESOURCE_METADATA));
    assertNull(parameters.get(PluginParameterKeys.RESOURCE_LINK_KEY));
    assertNotNull(MEDIA_RESOURCE_EXCEPTION, parameters.get(PluginParameterKeys.UNIFIED_ERROR_MESSAGE));

  }

  @Test
  public void shouldForwardTheTupleWhenNoResourceLinkFound() {
    Tuple anchorTuple = mock(TupleImpl.class);
    resourceProcessingBolt.execute(anchorTuple, stormTaskTuple);
    int expectedParametersSize = 1;
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
      when(thumbnail.getContentSize()).thenReturn(1L);
      when(thumbnail.getTargetName()).thenReturn(thumbnailName);
      when(thumbnail.getContentStream()).thenReturn(Mockito.mock(InputStream.class));
      list.add(thumbnail);
    }
    return list;

  }
}