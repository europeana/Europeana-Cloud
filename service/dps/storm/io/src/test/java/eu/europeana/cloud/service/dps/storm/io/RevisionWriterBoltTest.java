package eu.europeana.cloud.service.dps.storm.io;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.mcs.driver.RevisionServiceClient;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class RevisionWriterBoltTest {

  private final int retryAttemptsCount = Optional.ofNullable(RetryableMethodExecutor.OVERRIDE_ATTEMPT_COUNT).orElse(8);

  @Mock(name = "outputCollector")
  private OutputCollector outputCollector;

  @Mock(name = "revisionsClient")
  private RevisionServiceClient revisionServiceClient;

  @InjectMocks
  private RevisionWriterBolt revisionWriterBolt =
      new RevisionWriterBolt(new CassandraProperties(), "http://sample.ecloud.com/", "", "");

  @Captor
  private ArgumentCaptor<Revision> captor;


  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void nothingShouldBeAddedForEmptyRevisionsList() throws MCSException {
    Tuple anchorTuple = mock(TupleImpl.class);
    RevisionWriterBolt testMock = Mockito.spy(revisionWriterBolt);
    StormTaskTuple stormTaskTuple = new StormTaskTuple();
    stormTaskTuple.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
    testMock.execute(anchorTuple, stormTaskTuple);

    Mockito.verify(revisionServiceClient, Mockito.times(0))
           .addRevision(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(Revision.class));
    Mockito.verify(outputCollector, Mockito.times(1))
           .emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), Mockito.any(List.class));
  }

  @Test
  public void methodForAddingRevisionsShouldBeExecuted() throws MCSException {
    Tuple anchorTuple = mock(TupleImpl.class);
    RevisionWriterBolt testMock = Mockito.spy(revisionWriterBolt);
    testMock.execute(anchorTuple, prepareTuple());
    Mockito.verify(revisionServiceClient, Mockito.times(1)).addRevision(any(), any(), any(), any(Revision.class));
    Mockito.verify(outputCollector, Mockito.times(1))
           .emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), Mockito.any(List.class));
  }

  @Test
  public void methodForAddingRevisionsShouldBeExecutedForDeletedRecord() throws MCSException {
    Tuple anchorTuple = mock(TupleImpl.class);
    RevisionWriterBolt testMock = Mockito.spy(revisionWriterBolt);
    StormTaskTuple stormTaskTuple = prepareTuple();
    stormTaskTuple.setMarkedAsDeleted(true);

    testMock.execute(anchorTuple, stormTaskTuple);

    Mockito.verify(revisionServiceClient, Mockito.times(1)).addRevision(any(), any(), any(), captor.capture());
    assertTrue(captor.getValue().isDeleted());
    Mockito.verify(outputCollector, Mockito.times(1))
           .emit(eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), Mockito.any(List.class));

  }

  @Test
  public void malformedUrlExceptionShouldBeHandled() throws MCSException {
    Tuple anchorTuple = mock(TupleImpl.class);
    RevisionWriterBolt testMock = Mockito.spy(revisionWriterBolt);
    testMock.execute(anchorTuple, prepareTupleWithMalformedURL());
    Mockito.verify(revisionServiceClient, Mockito.times(0))
           .addRevision(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.any(Revision.class));
    Mockito.verify(outputCollector, Mockito.times(1))
           .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), Mockito.any(List.class));
  }

  @Test
  public void mcsExceptionShouldBeHandledWithRetries() throws MCSException {
    Tuple anchorTuple = mock(TupleImpl.class);
    Mockito.when(revisionServiceClient.addRevision(any(), any(), any(), any(Revision.class))).thenThrow(MCSException.class);
    RevisionWriterBolt testMock = Mockito.spy(revisionWriterBolt);
    testMock.execute(anchorTuple, prepareTuple());
    Mockito.verify(revisionServiceClient, Mockito.times(retryAttemptsCount))
           .addRevision(any(), any(), any(), any(Revision.class));
    Mockito.verify(outputCollector, Mockito.times(1))
           .emit(Mockito.eq(AbstractDpsBolt.NOTIFICATION_STREAM_NAME), any(Tuple.class), Mockito.any(List.class));

  }

  private StormTaskTuple prepareTuple() {
    StormTaskTuple tuple = new StormTaskTuple(123L, "sampleTaskName", "http://inputFileUrl", null, prepareTaskParameters(),
        new Revision());
    tuple.addParameter(PluginParameterKeys.OUTPUT_URL, "http://sampleFileUrl");

    return tuple;
  }

  private StormTaskTuple prepareTupleWithMalformedURL() {
    StormTaskTuple tuple = new StormTaskTuple(123L, "sampleTaskName", "http://inputFileUrl", null, prepareTaskParameters(),
        new Revision());
    tuple.addParameter(PluginParameterKeys.OUTPUT_URL, "malformedURL");
    return tuple;
  }

  Map<String, String> prepareTaskParameters() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
    return parameters;
  }
}
