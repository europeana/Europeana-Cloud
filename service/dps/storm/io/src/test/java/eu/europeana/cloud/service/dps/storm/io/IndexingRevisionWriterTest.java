package eu.europeana.cloud.service.dps.storm.io;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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

public class IndexingRevisionWriterTest {

  @Mock(name = "outputCollector")
  private OutputCollector outputCollector;

  @Mock(name = "revisionsClient")
  private RevisionServiceClient revisionServiceClient;

  private final int retryAttemptsCount = Optional.ofNullable(RetryableMethodExecutor.OVERRIDE_ATTEMPT_COUNT).orElse(8);

  @InjectMocks
  private IndexingRevisionWriter indexingRevisionWriter = new IndexingRevisionWriter(new CassandraProperties(),
      "https://sample.ecloud.com/", "userName", "userPassword", "sampleMessage");

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
  }

  @Captor
  private ArgumentCaptor<Values> captor;


  @Test
  @SuppressWarnings("unchecked")
  public void nothingShouldBeAddedForEmptyRevisionsList() throws MCSException {
    Tuple anchorTuple = mock(TupleImpl.class);
    RevisionWriterBolt testMock = Mockito.spy(indexingRevisionWriter);
    testMock.execute(anchorTuple, prepareTupleWithEmptyRevisions());
    Mockito.verify(revisionServiceClient, Mockito.times(0))
           .addRevision(anyString(), anyString(), anyString(), Mockito.any(Revision.class));
    Mockito.verify(outputCollector, Mockito.times(0)).emit(Mockito.any(List.class));
    Mockito.verify(outputCollector, Mockito.times(1))
           .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), Mockito.any(List.class));
    Mockito.verify(outputCollector)
           .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), captor.capture());
    var list = captor.getValue();
    assertNotNull(list);
    assertEquals(3, list.size());
    Map<String, String> parameters = (Map<String, String>) list.get(1);
    assertEquals("SUCCESS", parameters.get(NotificationParameterKeys.STATE));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void methodForAddingRevisionsShouldBeExecuted() throws MCSException {
    Tuple anchorTuple = mock(TupleImpl.class);
    RevisionWriterBolt testMock = Mockito.spy(indexingRevisionWriter);
    testMock.execute(anchorTuple, prepareTuple());
    Mockito.verify(revisionServiceClient, Mockito.times(1)).addRevision(any(), any(), any(), Mockito.any(Revision.class));
    Mockito.verify(outputCollector, Mockito.times(0)).emit(Mockito.any(List.class));
    Mockito.verify(outputCollector, Mockito.times(1))
           .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), Mockito.any(List.class));
    Mockito.verify(outputCollector)
           .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), captor.capture());
    var list = captor.getValue();
    assertNotNull(list);
    assertEquals(3, list.size());
    Map<String, String> parameters = (Map<String, String>) list.get(1);
    assertEquals("SUCCESS", parameters.get(NotificationParameterKeys.STATE));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void malformedUrlExceptionShouldBeHandled() throws MCSException {
    Tuple anchorTuple = mock(TupleImpl.class);
    RevisionWriterBolt testMock = Mockito.spy(indexingRevisionWriter);
    testMock.execute(anchorTuple, prepareTupleWithMalformedURL());
    Mockito.verify(revisionServiceClient, Mockito.times(0))
           .addRevision(anyString(), anyString(), anyString(), Mockito.any(Revision.class));
    Mockito.verify(outputCollector, Mockito.times(1))
           .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), Mockito.any(List.class));
    Mockito.verify(outputCollector)
           .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), captor.capture());
    var list = captor.getValue();
    assertNotNull(list);
    assertEquals(3, list.size());
    Map<String, String> parameters = (Map<String, String>) list.get(1);
    assertEquals("ERROR", parameters.get(NotificationParameterKeys.STATE));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void mcsExceptionShouldBeHandledWithRetries() throws MCSException {
    Tuple anchorTuple = mock(TupleImpl.class);
    when(revisionServiceClient.addRevision(any(), any(), any(), Mockito.any(Revision.class)))
           .thenThrow(MCSException.class);
    RevisionWriterBolt testMock = Mockito.spy(indexingRevisionWriter);
    testMock.execute(anchorTuple, prepareTuple());
    Mockito.verify(revisionServiceClient, Mockito.times(retryAttemptsCount))
           .addRevision(any(), any(), any(), Mockito.any(Revision.class));
    Mockito.verify(outputCollector, Mockito.times(1))
           .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), Mockito.any(List.class));
  }

  private StormTaskTuple prepareTuple() {
    return new StormTaskTuple(123L, "sampleTaskName", "http://inputFileUrl", null, prepareTaskParameters(), new Revision());
  }

  private StormTaskTuple prepareTupleWithMalformedURL() {
    return new StormTaskTuple(123L, "sampleTaskName", "malformed", null, prepareTaskParameters(), new Revision());
  }

  private StormTaskTuple prepareTupleWithEmptyRevisions() {
    return new StormTaskTuple(123L, "sampleTaskName", "http://inputFileUrl", null, prepareTaskParameters(), null);
  }

  Map<String, String> prepareTaskParameters() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
    return parameters;
  }

}


