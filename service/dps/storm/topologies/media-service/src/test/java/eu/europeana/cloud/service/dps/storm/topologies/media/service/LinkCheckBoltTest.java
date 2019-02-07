package eu.europeana.cloud.service.dps.storm.topologies.media.service;

import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Values;
import org.hamcrest.core.StringContains;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Map;

import static eu.europeana.cloud.service.dps.PluginParameterKeys.RESOURCE_LINKS_COUNT;
import static eu.europeana.cloud.service.dps.PluginParameterKeys.RESOURCE_URL;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class LinkCheckBoltTest {

    @Mock(name = "outputCollector")
    private OutputCollector outputCollector;

    @Mock
    private LinkChecker linkChecker;

    @InjectMocks
    private LinkCheckBolt linkCheckBolt = new LinkCheckBolt(linkChecker);

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    @Test
    public void shouldEmitSameTupleWhenNoResourcesHasToBeChecked() {
        StormTaskTuple tuple = prepareTupleWithLinksCountEqualsToZero();
        linkCheckBolt.execute(tuple);
        verify(outputCollector, times(1)).emit(captor.capture());
        validateCapturedValues(captor);
    }

    @Test
    public void shouldCheckOneLinkWithoutEmittingTuple() throws Exception {
        StormTaskTuple tuple = prepareRandomTuple();
        linkCheckBolt.execute(tuple);
        verify(outputCollector, times(0)).emit(Mockito.anyList());
        verify(linkChecker, times(1)).check(tuple.getParameter(PluginParameterKeys.RESOURCE_URL));
    }

    @Test
    public void shouldEmitTupleAfterCheckingAllResourcesFromFile() throws Exception {
        StormTaskTuple tuple = prepareRandomTuple();
        linkCheckBolt.execute(tuple);
        verify(outputCollector, times(0)).emit(Mockito.anyList());
        verify(linkChecker, times(1)).check(tuple.getParameter(PluginParameterKeys.RESOURCE_URL));
        linkCheckBolt.execute(tuple);
        verify(outputCollector, times(0)).emit(Mockito.anyList());
        verify(linkChecker, times(2)).check(tuple.getParameter(PluginParameterKeys.RESOURCE_URL));
        linkCheckBolt.execute(tuple);
        verify(outputCollector, times(0)).emit(Mockito.anyList());
        verify(linkChecker, times(3)).check(tuple.getParameter(PluginParameterKeys.RESOURCE_URL));
        linkCheckBolt.execute(tuple);
        verify(outputCollector, times(0)).emit(Mockito.anyList());
        verify(linkChecker, times(4)).check(tuple.getParameter(PluginParameterKeys.RESOURCE_URL));
        linkCheckBolt.execute(tuple);
        verify(outputCollector, times(1)).emit(Mockito.anyList());
        verify(linkChecker, times(5)).check(Mockito.anyString());
    }

    @Test
    public void shouldEmitTupleWithErrorIncluded() throws Exception {
        when(linkChecker.check(Mockito.anyString())).thenReturn(500,501,502,505,507);
        StormTaskTuple tuple = prepareRandomTuple();
        linkCheckBolt.execute(tuple);
        linkCheckBolt.execute(tuple);
        linkCheckBolt.execute(tuple);
        linkCheckBolt.execute(tuple);
        linkCheckBolt.execute(tuple);
        verify(outputCollector, times(1)).emit(captor.capture());
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
        Map<String, String> parameters = (Map) values.get(4);
        assertNotNull(parameters);
        assertEquals(0, parameters.size());
        assertNull(parameters.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
        assertNull(parameters.get(PluginParameterKeys.RESOURCE_URL));
        assertEquals("ecloudFileUrl", values.get(2));
    }

    private void validateCapturedValuesForError(ArgumentCaptor<Values> captor) {
        Values values = captor.getValue();
        Map<String, String> parameters = (Map) values.get(4);
        assertNotNull(parameters);
        assertEquals(2, parameters.size());
        assertNotNull(parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE));
        assertThat(parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE), StringContains.containsString("500"));
        assertThat(parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE), StringContains.containsString("501"));
        assertThat(parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE), StringContains.containsString("502"));
        assertThat(parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE), StringContains.containsString("505"));
        assertThat(parameters.get(PluginParameterKeys.EXCEPTION_ERROR_MESSAGE), StringContains.containsString("507"));

        assertNotNull(parameters.get(PluginParameterKeys.UNIFIED_ERROR_MESSAGE));
        assertEquals("ecloudFileUrl", values.get(2));
        assertNull(parameters.get(PluginParameterKeys.RESOURCE_LINKS_COUNT));
        assertNull(parameters.get(PluginParameterKeys.RESOURCE_URL));
        assertEquals("ecloudFileUrl", values.get(2));
    }
}