package eu.europeana.cloud.service.dps.storm.topologies.media.service;


import com.amazonaws.services.s3.model.ObjectMetadata;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.tuple.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.metis.mediaprocessing.MediaExtractor;
import eu.europeana.metis.mediaprocessing.MediaProcessorFactory;
import eu.europeana.metis.mediaprocessing.RdfConverterFactory;
import eu.europeana.metis.mediaprocessing.RdfDeserializer;
import eu.europeana.metis.mediaprocessing.exception.MediaExtractionException;
import eu.europeana.metis.mediaprocessing.exception.MediaProcessorException;
import eu.europeana.metis.mediaprocessing.model.RdfResourceEntry;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.InputStream;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class EDMObjectProcessorBoltTest {

  @Captor
  ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

  @Mock(name = "outputCollector")
  private OutputCollector outputCollector;

  @Mock(name = "fileClient")
  private FileServiceClient fileClient;

  @SuppressWarnings("unused") //Field is used on runtime - by @InjectMocks
  @Mock(name = "taskStatusChecker")
  private TaskStatusChecker taskStatusChecker;

  @Spy
  private transient MediaExtractor mediaExtractor;

  @Spy
  private transient RdfDeserializer rdfDeserializer;

  private final AmazonClient amazonClient = Mockito.mock(AmazonClient.class);

  @InjectMocks
  private final EDMObjectProcessorBolt edmObjectProcessorBolt = new EDMObjectProcessorBolt(
          new CassandraProperties(), "MCS_URL", "user", "password", amazonClient);

  @BeforeEach
  void init() throws MediaProcessorException {
    edmObjectProcessorBolt.prepare();
    mediaExtractor = new MediaProcessorFactory().createMediaExtractor();
    rdfDeserializer = new RdfConverterFactory().createRdfDeserializer();
    // For some reason test crashes without this even though we use MockitoExtension?
    MockitoAnnotations.initMocks(this); // initialize all the @Mock objects
  }

  @Test
  void shouldDoProperEmissionInCaseOfFileWithSingleResource() throws Exception {
    //given
    try (InputStream stream = this.getClass().getResourceAsStream("/files/fileWithSingleResource.xml")) {
      when(fileClient.getFile(anyString())).thenReturn(stream);
      StormTaskTuple tuple = new StormTaskTuple();
      tuple.setRecordUri("example");
      //
      Tuple anchorTuple = mock(TupleImpl.class);

      //when
      edmObjectProcessorBolt.execute(anchorTuple, tuple);
      //then
      verify(outputCollector, times(1)).emit(eq(EDMObjectProcessorBolt.EDM_OBJECT_ENRICHMENT_STREAM_NAME), any(Tuple.class),
              captor.capture());
      verify(outputCollector, times(1)).emit(any(Tuple.class), captor.capture());
      verify(outputCollector, times(1)).ack(any(Tuple.class));
      Values valuesForEnrichmentBolt = captor.getValue();
      Values valuesForParseFileBolt = captor.getValue();
      //
      Map<String, String> parametersForEnrichmentBolt = (Map) valuesForEnrichmentBolt.get(5);
      Map<String, String> parametersForParseFileBolt = (Map) valuesForParseFileBolt.get(5);
      assertEquals("1", parametersForParseFileBolt.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
      assertEquals("1", parametersForEnrichmentBolt.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
    }
  }

  @Test
  void shouldDoProperEmissionInCaseOfResourceProcessingExceptionForSingleResourceFile() throws Exception {
    //given
    try (InputStream stream = this.getClass().getResourceAsStream("/files/fileWithSingleResource.xml")) {
      when(fileClient.getFile(anyString())).thenReturn(stream);
      doThrow(MediaExtractionException.class).when(mediaExtractor)
              .performMediaExtraction(any(RdfResourceEntry.class), anyBoolean());

      StormTaskTuple tuple = new StormTaskTuple();
      tuple.setRecordUri("example");
      //
      Tuple anchorTuple = mock(TupleImpl.class);

      //when
      edmObjectProcessorBolt.execute(anchorTuple, tuple);
      //then
      verify(outputCollector, times(1)).emit(eq(EDMObjectProcessorBolt.EDM_OBJECT_ENRICHMENT_STREAM_NAME), any(Tuple.class),
              captor.capture());
      verify(outputCollector, times(1)).emit(any(Tuple.class), captor.capture());
      verify(outputCollector, times(1)).ack(any(Tuple.class));
      Values valuesForEnrichmentBolt = captor.getValue();
      Values valuesForParseFileBolt = captor.getValue();

      Map<String, String> parametersForEnrichmentBolt = (Map) valuesForEnrichmentBolt.get(5);
      Map<String, String> parametersForParseFileBolt = (Map) valuesForParseFileBolt.get(5);
      assertEquals("1", parametersForParseFileBolt.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
      assertEquals("1", parametersForEnrichmentBolt.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
    }
  }

  @Test
  void shouldDoProperEmissionInCaseOfResourceProcessingExceptionForTwoResourcesFile() throws Exception {
    //given
    try (InputStream stream = this.getClass().getResourceAsStream("/files/fileWithTwoResources.xml")) {
      when(fileClient.getFile(anyString())).thenReturn(stream);
      doThrow(MediaExtractionException.class).when(mediaExtractor)
              .performMediaExtraction(any(RdfResourceEntry.class), anyBoolean());

      StormTaskTuple tuple = new StormTaskTuple();
      tuple.setRecordUri("example");
      //
      Tuple anchorTuple = mock(TupleImpl.class);

      //when
      edmObjectProcessorBolt.execute(anchorTuple, tuple);
      //then
      verify(outputCollector, times(1)).emit(eq(EDMObjectProcessorBolt.EDM_OBJECT_ENRICHMENT_STREAM_NAME), any(Tuple.class),
              captor.capture());
      verify(outputCollector, times(1)).emit(any(Tuple.class), captor.capture());
      verify(outputCollector, times(1)).ack(any(Tuple.class));
      Values valuesForEnrichmentBolt = captor.getValue();
      Values valuesForParseFileBolt = captor.getValue();

      Map<String, String> parametersForEnrichmentBolt = (Map) valuesForEnrichmentBolt.get(5);
      Map<String, String> parametersForParseFileBolt = (Map) valuesForParseFileBolt.get(5);
      assertEquals("2", parametersForParseFileBolt.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
      assertEquals("2", parametersForEnrichmentBolt.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
    }
  }

  @Test
  void shouldDoProperEmissionInCaseOfFileContainingNoMainThumbnailResource() throws Exception {
    //given
    try (InputStream stream = this.getClass().getResourceAsStream("/files/fileWithTwoResources.xml")) {
      when(fileClient.getFile(anyString())).thenReturn(stream);

      doReturn(null).when(rdfDeserializer).getMainThumbnailResourceForMediaExtraction(any(byte[].class));

      StormTaskTuple tuple = new StormTaskTuple();
      tuple.setRecordUri("example");
      //
      Tuple anchorTuple = mock(TupleImpl.class);

      //when
      edmObjectProcessorBolt.execute(anchorTuple, tuple);
      //then
      verify(outputCollector, times(0)).emit(eq(EDMObjectProcessorBolt.EDM_OBJECT_ENRICHMENT_STREAM_NAME), any(Tuple.class),
          captor.capture());
      verify(outputCollector, times(1)).emit(any(Tuple.class), captor.capture());
      verify(outputCollector, times(1)).ack(any(Tuple.class));
      Values valuesForParseFileBolt = captor.getValue();

      Map<String, String> parametersForParseFileBolt = (Map) valuesForParseFileBolt.get(5);
      assertEquals("1", parametersForParseFileBolt.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
    }
  }

  @Test
  void shouldDoProperEmissionWhileThumbnailStoringFailure() throws Exception {
    //given
    try (InputStream stream = this.getClass().getResourceAsStream("/files/fileWithTwoResources.xml")) {
      when(fileClient.getFile(anyString())).thenReturn(stream);

      when(amazonClient.putObject(anyString(), any(InputStream.class), any(ObjectMetadata.class))).thenThrow(
              new RuntimeException());

      StormTaskTuple tuple = new StormTaskTuple();
      tuple.setRecordUri("example");
      //
      Tuple anchorTuple = mock(TupleImpl.class);

      //when
      edmObjectProcessorBolt.execute(anchorTuple, tuple);
      //then
      verify(outputCollector, times(1)).emit(eq(EDMObjectProcessorBolt.EDM_OBJECT_ENRICHMENT_STREAM_NAME), any(Tuple.class),
          captor.capture());
      verify(outputCollector, times(1)).emit(any(Tuple.class), captor.capture());
      verify(outputCollector, times(1)).ack(any(Tuple.class));
      Values valuesForParseFileBolt = captor.getValue();

      Map<String, String> parametersForParseFileBolt = (Map) valuesForParseFileBolt.get(5);
      assertEquals("2", parametersForParseFileBolt.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
      assertEquals("media resource exception", parametersForParseFileBolt.get(PluginParameterKeys.UNIFIED_ERROR_MESSAGE));
      assertTrue(parametersForParseFileBolt.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE).contains("Error while uploading"));
    }
  }
}