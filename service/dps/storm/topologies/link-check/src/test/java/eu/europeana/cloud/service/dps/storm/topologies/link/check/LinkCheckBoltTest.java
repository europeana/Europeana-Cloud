package eu.europeana.cloud.service.dps.storm.topologies.link.check;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.RESOURCE_LINKS_COUNT;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.RESOURCE_URL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import eu.europeana.cloud.common.properties.CassandraProperties;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.metis.mediaprocessing.LinkChecker;
import eu.europeana.metis.mediaprocessing.exception.LinkCheckingException;
import java.util.HashMap;
import java.util.Map;
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

public class LinkCheckBoltTest {

  @Mock(name = "outputCollector")
  private OutputCollector outputCollector;

  @Mock
  private LinkChecker linkChecker;

  @InjectMocks
  private LinkCheckBolt linkCheckBolt = new LinkCheckBolt(new CassandraProperties());

  @Captor
  ArgumentCaptor<Values> captor;

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
    linkCheckBolt.cache = new HashMap<>();
  }

  @Test
  public void shouldEmitSameTupleWhenNoResourcesHasToBeChecked() {
    Tuple anchorTuple = mock(TupleImpl.class);
    StormTaskTuple tuple = prepareTupleWithLinksCountEqualsToZero();
    linkCheckBolt.execute(anchorTuple, tuple);
    verify(outputCollector, times(1)).emit(eq("NotificationStream"), eq(anchorTuple), captor.capture());
    validateCapturedValues(captor, false);
  }

  @Test
  public void shouldEmitSameTupleWhenRecordIsDeleted() {
    Tuple anchorTuple = mock(TupleImpl.class);
    StormTaskTuple tuple = prepareTupleWithDeletedRecord();

    linkCheckBolt.execute(anchorTuple, tuple);

    verify(outputCollector).emit(eq("NotificationStream"), eq(anchorTuple), captor.capture());
    validateCapturedValues(captor, true);
  }

  @Test
  public void shouldCheckOneLinkWithoutEmittingTuple() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    StormTaskTuple tuple = prepareRandomTuple();
    linkCheckBolt.execute(anchorTuple, tuple);
    verify(outputCollector, times(0)).emit(eq("NotificationStream"), any(Tuple.class), Mockito.anyList());
    verify(linkChecker, times(1)).performLinkChecking(tuple.getParameter(PluginParameterKeys.RESOURCE_URL));
  }

  @Test
  public void shouldEmitTupleAfterCheckingAllResourcesFromFile() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    StormTaskTuple tuple = prepareRandomTuple();
    linkCheckBolt.execute(anchorTuple, tuple);
    verify(outputCollector, times(0)).emit(eq("NotificationStream"), any(Tuple.class), Mockito.anyList());
    verify(linkChecker, times(1)).performLinkChecking(tuple.getParameter(PluginParameterKeys.RESOURCE_URL));
    linkCheckBolt.execute(anchorTuple, tuple);
    verify(outputCollector, times(0)).emit(eq("NotificationStream"), any(Tuple.class), Mockito.anyList());
    verify(linkChecker, times(2)).performLinkChecking(tuple.getParameter(PluginParameterKeys.RESOURCE_URL));
    linkCheckBolt.execute(anchorTuple, tuple);
    verify(outputCollector, times(0)).emit(eq("NotificationStream"), any(Tuple.class), Mockito.anyList());
    verify(linkChecker, times(3)).performLinkChecking(tuple.getParameter(PluginParameterKeys.RESOURCE_URL));
    linkCheckBolt.execute(anchorTuple, tuple);
    verify(outputCollector, times(0)).emit(eq("NotificationStream"), any(Tuple.class), Mockito.anyList());
    verify(linkChecker, times(4)).performLinkChecking(tuple.getParameter(PluginParameterKeys.RESOURCE_URL));
    linkCheckBolt.execute(anchorTuple, tuple);
    verify(outputCollector, times(1)).emit(eq("NotificationStream"), any(Tuple.class), Mockito.anyList());
    verify(linkChecker, times(5)).performLinkChecking(Mockito.anyString());
  }

  @Test
  public void shouldEmitTupleWithErrorIncluded() throws Exception {
    Tuple anchorTuple = mock(TupleImpl.class);
    doThrow(new LinkCheckingException(new Throwable())).when(linkChecker).performLinkChecking(Mockito.anyString());
    StormTaskTuple tuple = prepareRandomTuple();
    linkCheckBolt.execute(anchorTuple, tuple);
    linkCheckBolt.execute(anchorTuple, tuple);
    linkCheckBolt.execute(anchorTuple, tuple);
    linkCheckBolt.execute(anchorTuple, tuple);
    linkCheckBolt.execute(anchorTuple, tuple);
    verify(outputCollector, times(1)).emit(eq("NotificationStream"), any(Tuple.class), captor.capture());
    validateCapturedValuesForError(captor);
  }


  private StormTaskTuple prepareRandomTuple() {
    StormTaskTuple tuple = new StormTaskTuple();
    tuple.setFileUrl("ecloudFileUrl");
    tuple.addParameter(RESOURCE_LINKS_COUNT, 5 + "");
    tuple.addParameter(RESOURCE_URL, "resourceUrl");
    tuple.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
    return tuple;
  }

  private StormTaskTuple prepareTupleWithLinksCountEqualsToZero() {
    StormTaskTuple tuple = new StormTaskTuple();
    tuple.setFileUrl("ecloudFileUrl");
    tuple.addParameter(RESOURCE_LINKS_COUNT, 0 + "");
    tuple.addParameter(RESOURCE_URL, "resourceUrl");
    tuple.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
    return tuple;
  }

  private StormTaskTuple prepareTupleWithDeletedRecord() {
    StormTaskTuple tuple = new StormTaskTuple();
    tuple.setFileUrl("ecloudFileUrl");
    tuple.addParameter(PluginParameterKeys.MESSAGE_PROCESSING_START_TIME_IN_MS, "1");
    tuple.setMarkedAsDeleted(true);
    return tuple;
  }

  @SuppressWarnings("unchecked")
  private void validateCapturedValues(ArgumentCaptor<Values> captor, boolean hasDeletedInfo) {
    var parameters = (Map<String, String>) captor.getValue().get(1);
    assertNotNull(parameters);
    assertEquals(hasDeletedInfo ? 7 : 6, parameters.size());
    assertNull(parameters.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
    assertNull(parameters.get(PluginParameterKeys.RESOURCE_URL));
    assertEquals("ecloudFileUrl", parameters.get(NotificationParameterKeys.RESOURCE));
  }

  @SuppressWarnings("unchecked")
  private void validateCapturedValuesForError(ArgumentCaptor<Values> captor) {
    var parameters = (Map<String, String>) captor.getValue().get(1);
    assertNotNull(parameters);
    assertEquals(8, parameters.size());
    assertNotNull(parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE));
    assertEquals(5, parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE).split(",").length);
    assertNotNull(parameters.get(PluginParameterKeys.UNIFIED_ERROR_MESSAGE));
    assertEquals("ecloudFileUrl", parameters.get(NotificationParameterKeys.RESOURCE));
    assertNull(parameters.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
    assertNull(parameters.get(PluginParameterKeys.RESOURCE_URL));
  }
}
