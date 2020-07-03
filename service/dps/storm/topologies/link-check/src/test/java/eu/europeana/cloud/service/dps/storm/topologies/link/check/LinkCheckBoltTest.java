package eu.europeana.cloud.service.dps.storm.topologies.link.check;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.metis.mediaprocessing.LinkChecker;
import eu.europeana.metis.mediaprocessing.exception.LinkCheckingException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.mockito.*;

import java.util.Map;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.RESOURCE_LINKS_COUNT;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.RESOURCE_URL;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class LinkCheckBoltTest {

    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @Mock
    private LinkChecker linkChecker;

    @InjectMocks
    private LinkCheckBolt linkCheckBolt = new LinkCheckBolt();

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldEmitSameTupleWhenNoResourcesHasToBeChecked() {
        Tuple anchorTuple = mock(TupleImpl.class);
        StormTaskTuple tuple = prepareTupleWithLinksCountEqualsToZero();
        linkCheckBolt.execute(anchorTuple, tuple);
        verify(outputCollector, times(1)).emit(eq("NotificationStream"), captor.capture());
        validateCapturedValues(captor);
    }

    @Test
    public void shouldCheckOneLinkWithoutEmittingTuple() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        StormTaskTuple tuple = prepareRandomTuple();
        linkCheckBolt.execute(anchorTuple, tuple);
        verify(outputCollector, times(0)).emit(eq("NotificationStream"), Mockito.anyList());
        verify(linkChecker, times(1)).performLinkChecking(tuple.getParameter(PluginParameterKeys.RESOURCE_URL));
    }

    @Test
    public void shouldEmitTupleAfterCheckingAllResourcesFromFile() throws Exception {
        Tuple anchorTuple = mock(TupleImpl.class);
        StormTaskTuple tuple = prepareRandomTuple();
        linkCheckBolt.execute(anchorTuple, tuple);
        verify(outputCollector, times(0)).emit(eq("NotificationStream"), Mockito.anyList());
        verify(linkChecker, times(1)).performLinkChecking(tuple.getParameter(PluginParameterKeys.RESOURCE_URL));
        linkCheckBolt.execute(anchorTuple, tuple);
        verify(outputCollector, times(0)).emit(eq("NotificationStream"), Mockito.anyList());
        verify(linkChecker, times(2)).performLinkChecking(tuple.getParameter(PluginParameterKeys.RESOURCE_URL));
        linkCheckBolt.execute(anchorTuple, tuple);
        verify(outputCollector, times(0)).emit(eq("NotificationStream"), Mockito.anyList());
        verify(linkChecker, times(3)).performLinkChecking(tuple.getParameter(PluginParameterKeys.RESOURCE_URL));
        linkCheckBolt.execute(anchorTuple, tuple);
        verify(outputCollector, times(0)).emit(eq("NotificationStream"), Mockito.anyList());
        verify(linkChecker, times(4)).performLinkChecking(tuple.getParameter(PluginParameterKeys.RESOURCE_URL));
        linkCheckBolt.execute(anchorTuple, tuple);
        verify(outputCollector, times(1)).emit(eq("NotificationStream"), Mockito.anyList());
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
        verify(outputCollector, times(1)).emit(eq("NotificationStream"), captor.capture());
        validateCapturedValuesForError(captor);
    }


    private StormTaskTuple prepareRandomTuple() {
        StormTaskTuple tuple = new StormTaskTuple();
        tuple.setFileUrl("ecloudFileUrl");
        tuple.addParameter(RESOURCE_LINKS_COUNT, 5 + "");
        tuple.addParameter(RESOURCE_URL, "resourceUrl");
        return tuple;
    }

    private StormTaskTuple prepareTupleWithLinksCountEqualsToZero() {
        StormTaskTuple tuple = new StormTaskTuple();
        tuple.setFileUrl("ecloudFileUrl");
        tuple.addParameter(RESOURCE_LINKS_COUNT, 0 + "");
        tuple.addParameter(RESOURCE_URL, "resourceUrl");
        return tuple;
    }

    private void validateCapturedValues(ArgumentCaptor<Values> captor) {
        Values values = captor.getValue();
        Map<String, String> parameters = (Map) values.get(2);
        assertNotNull(parameters);
        assertEquals(5, parameters.size());
        assertNull(parameters.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
        assertNull(parameters.get(PluginParameterKeys.RESOURCE_URL));
        assertEquals("ecloudFileUrl", parameters.get("resource"));
    }

    private void validateCapturedValuesForError(ArgumentCaptor<Values> captor) {
        Values values = captor.getValue();
        Map<String, String> parameters = (Map) values.get(2);
        assertNotNull(parameters);
        assertEquals(7, parameters.size());
        assertNotNull(parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE));
        assertEquals(5, parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE).split(",").length);
        assertNotNull(parameters.get(PluginParameterKeys.UNIFIED_ERROR_MESSAGE));
        assertEquals("ecloudFileUrl", parameters.get("resource"));
        assertNull(parameters.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
        assertNull(parameters.get(PluginParameterKeys.RESOURCE_URL));
    }
}
