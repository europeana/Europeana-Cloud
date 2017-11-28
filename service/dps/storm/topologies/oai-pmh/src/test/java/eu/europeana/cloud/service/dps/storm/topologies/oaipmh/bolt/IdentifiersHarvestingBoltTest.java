package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt;

import com.lyncode.xoai.model.oaipmh.Header;
import com.lyncode.xoai.serviceprovider.ServiceProvider;
import com.lyncode.xoai.serviceprovider.exceptions.BadArgumentException;
import com.lyncode.xoai.serviceprovider.exceptions.InvalidOAIResponse;
import com.lyncode.xoai.serviceprovider.parameters.ListIdentifiersParameters;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.helpers.SourceProvider;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.*;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class IdentifiersHarvestingBoltTest {

    @Mock(name = "outputCollector")
    private OutputCollector oc;

    @Mock(name = "sourceProvider")
    private SourceProvider sourceProvider;

    @Mock
    private ServiceProvider source;

    @InjectMocks
    private IdentifiersHarvestingBolt instance = new IdentifiersHarvestingBolt();

    private final int TASK_ID = 1;

    private final String TASK_NAME = "TASK_NAME";

    private final String OAI_URL = "http://localhost/oai-pmh-repository.xml";

    private final String SCHEMA = "oai_dc";

    private final String SET1 = "set1";

    private final String SET2 = "set2";

    private final String ID1 = "oai:localhost:1";

    private final String ID2 = "oai:localhost:2";

    private final Date DATE1 = new GregorianCalendar(2017, Calendar.JANUARY, 1).getTime();

    private final Date DATE2 = new GregorianCalendar(2017, Calendar.FEBRUARY, 1).getTime();

    private Set<Header> headers;

    private Iterator<Header> iterator;


    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    private Set<String> prepareSets(String set) {
        Set<String> sets = new HashSet<>();
        sets.add(set);
        return sets;
    }

    private StormTaskTuple configureStormTaskTuple(String url, String schema, Set<String> sets, Set<String> excludedSets, Date from, Date until) {
        OAIPMHHarvestingDetails sourceDetails = new OAIPMHHarvestingDetails(schema);
        sourceDetails.setSets(sets);
        sourceDetails.setExcludedSets(excludedSets);
        sourceDetails.setDateFrom(from);
        sourceDetails.setDateUntil(until);
        initHeaders(sourceDetails);
        mockSource(iterator);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, null, null, new HashMap<String, String>(), new Revision(), sourceDetails);
        tuple.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, url);
        return tuple;
    }

    @Test
    public void testRetriesFailed() {
        //given
        StormTaskTuple tuple = configureStormTaskTuple(OAI_URL, SCHEMA, null, null, null, null);
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);

        Iterator<Header> mockIterator = Mockito.mock(Iterator.class);
        mockSource(mockIterator);
        when(mockIterator.hasNext()).thenThrow(new InvalidOAIResponse());

        //when
        instance.execute(tuple);

        //then
        verify(oc, times(0)).emit(any(Tuple.class), captor.capture());
        verify(oc, times(1)).emit(eq(NOTIFICATION_STREAM_NAME), any(Tuple.class), anyList());
        verifyNoMoreInteractions(oc);
    }

    @Test
    public void testRetriesSuccessful() {
        //given
        StormTaskTuple tuple = configureStormTaskTuple(OAI_URL, SCHEMA, null, null, null, null);
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);

        Iterator<Header> mockIterator = Mockito.mock(Iterator.class);
        mockSource(mockIterator);
        when(mockIterator.hasNext()).thenThrow(new InvalidOAIResponse()).thenThrow(new InvalidOAIResponse()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(mockIterator.next()).thenReturn(iterator.next()).thenReturn(iterator.next());

        //when
        instance.execute(tuple);
        //then
        verify(oc, times(2)).emit(any(Tuple.class), captor.capture());

        List<Values> values = captor.getAllValues();
        assertThat(values.size(), is(2));

        Set<String> identifiers = new HashSet<>();
        identifiers.add(((HashMap<String, String>) values.get(0).get(4)).get(PluginParameterKeys.OAI_IDENTIFIER));
        identifiers.add(((HashMap<String, String>) values.get(1).get(4)).get(PluginParameterKeys.OAI_IDENTIFIER));
        assertTrue(identifiers.contains(ID1));
        assertTrue(identifiers.contains(ID2));

        verifyNoMoreInteractions(oc);
    }

    @Test
    public void testInvalidURL() {
        //given
        StormTaskTuple tuple = configureStormTaskTuple(null, SCHEMA, null, null, null, null);
        //when
        instance.execute(tuple);
        //then
        verify(oc, atMost(0)).emit(any(Tuple.class), captor.capture());
        NotificationTuple nt = NotificationTuple.prepareNotification(TASK_ID,
                null, States.ERROR, "Source is not configured.", "{DPS_TASK_INPUT_DATA=null}");
        verifyExecutionTimes(1, nt);
    }

    private void verifyExecutionTimes(int times, NotificationTuple nt) {
        verify(oc, times(times)).emit(eq(NOTIFICATION_STREAM_NAME), any(Tuple.class), eq(nt.toStormTuple()));
    }

    @Test
    public void testInvalidSchema() {
        //given
        StormTaskTuple tuple = configureStormTaskTuple(OAI_URL, null, null, null, null, null);
        //when
        instance.execute(tuple);
        //then
        verify(oc, atMost(0)).emit(any(Tuple.class), captor.capture());
        NotificationTuple nt = NotificationTuple.prepareNotification(TASK_ID,
                null, States.ERROR, "Schema is not specified.", "{DPS_TASK_INPUT_DATA=" + OAI_URL + "}");
        verifyExecutionTimes(1, nt);
    }


    @Test
    public void testNullHarvestingDetails() {
        //given
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, null, null, null, new HashMap<String, String>(), null, null);
        //when
        instance.execute(tuple);
        //then
        verify(oc, atMost(0)).emit(any(Tuple.class), captor.capture());
        NotificationTuple nt = NotificationTuple.prepareNotification(TASK_ID,
                null, States.ERROR, "Harvesting details object is null!", "{}");
        verifyExecutionTimes(1, nt);
    }


    @Test
    public void testRunTimeException() {
        //given
        StormTaskTuple tuple = configureStormTaskTuple(OAI_URL, SCHEMA, null, null, DATE1, DATE2);
        when(sourceProvider.provide(anyString())).thenThrow(new RuntimeException("Runtime exception..."));
        //when
        instance.execute(tuple);
        //then
        verify(oc, atMost(0)).emit(any(Tuple.class), captor.capture());
        NotificationTuple nt = NotificationTuple.prepareNotification(TASK_ID,
                null, States.ERROR, "Runtime exception...", "{DPS_TASK_INPUT_DATA=" + OAI_URL + "}");
        verifyExecutionTimes(1, nt);
    }

    @Test
    public void testBadArgumentException() {
        //given
        StormTaskTuple tuple = configureStormTaskTuple(OAI_URL, SCHEMA, null, null, DATE1, DATE2);
        when(sourceProvider.provide(anyString())).thenThrow(new RuntimeException("Bad Argument Exception..."));
        //when
        instance.execute(tuple);
        //then
        verify(oc, atMost(0)).emit(any(Tuple.class), captor.capture());
        NotificationTuple nt = NotificationTuple.prepareNotification(TASK_ID,
                null, States.ERROR, "Bad Argument Exception...", "{DPS_TASK_INPUT_DATA=" + OAI_URL + "}");
        verifyExecutionTimes(1, nt);
    }


    @Test
    public void testDateFromSpecified() {
        //given
        StormTaskTuple tuple = configureStormTaskTuple(OAI_URL, SCHEMA, null, null, DATE2, null);
        //when
        instance.execute(tuple);
        //then
        verify(oc, times(1)).emit(any(Tuple.class), captor.capture());
        List<Values> values = captor.getAllValues();
        assertThat(values.size(), is(1));
        assertEquals(((HashMap<String, String>) values.get(0).get(4)).get(PluginParameterKeys.OAI_IDENTIFIER), ID2);
        verifyNoInteraction();
    }

    @Test
    public void testDateUntilSpecified() {
        //given
        StormTaskTuple tuple = configureStormTaskTuple(OAI_URL, SCHEMA, null, null, null, DATE1);
        //when
        instance.execute(tuple);
        //then
        verify(oc, times(1)).emit(any(Tuple.class), captor.capture());
        List<Values> values = captor.getAllValues();
        assertThat(values.size(), is(1));

        assertEquals(((HashMap<String, String>) values.get(0).get(4)).get(PluginParameterKeys.OAI_IDENTIFIER), ID1);
        verifyNoInteraction();
    }

    @Test
    public void testDatesSpecified() {
        //given
        StormTaskTuple tuple = configureStormTaskTuple(OAI_URL, SCHEMA, null, null, DATE1, DATE2);
        //when
        instance.execute(tuple);
        //then
        verify(oc, times(2)).emit(any(Tuple.class), captor.capture());
        List<Values> values = captor.getAllValues();
        assertThat(values.size(), is(2));

        Set<String> identifiers = new HashSet<>();
        identifiers.add(((HashMap<String, String>) values.get(0).get(4)).get(PluginParameterKeys.OAI_IDENTIFIER));
        identifiers.add(((HashMap<String, String>) values.get(1).get(4)).get(PluginParameterKeys.OAI_IDENTIFIER));
        assertTrue(identifiers.contains(ID1));
        assertTrue(identifiers.contains(ID2));
        verifyNoInteraction();
    }

    @Test
    public void testHarvestingAll() {
        //given
        StormTaskTuple tuple = configureStormTaskTuple(OAI_URL, SCHEMA, null, null, null, null);
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);
        //when
        instance.execute(tuple);
        //then
        verify(oc, times(2)).emit(any(Tuple.class), captor.capture());

        List<Values> values = captor.getAllValues();
        assertThat(values.size(), is(2));

        Set<String> identifiers = new HashSet<>();
        identifiers.add(((HashMap<String, String>) values.get(0).get(4)).get(PluginParameterKeys.OAI_IDENTIFIER));
        identifiers.add(((HashMap<String, String>) values.get(1).get(4)).get(PluginParameterKeys.OAI_IDENTIFIER));
        assertTrue(identifiers.contains(ID1));
        assertTrue(identifiers.contains(ID2));

        verifyNoMoreInteractions(oc);
        verifyNoInteraction();
    }

    private void verifyNoInteraction() {
        verify(oc, times(0)).emit(eq("NotificationStream"), any(Tuple.class), Mockito.anyList());
    }

    @Test
    public void testSimpleHarvestingWhenExcludedSetSpecified() {
        //given
        StormTaskTuple tuple = configureStormTaskTuple(OAI_URL, SCHEMA, null, prepareSets(SET1), null, null);
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);
        //when
        instance.execute(tuple);
        //then
        verify(oc, times(1)).emit(any(Tuple.class), captor.capture());

        List<Values> values = captor.getAllValues();
        assertThat(values.size(), is(1));
        assertThat(((HashMap<String, String>) values.get(0).get(4)).get(PluginParameterKeys.OAI_IDENTIFIER), is(ID2));

        verifyNoMoreInteractions(oc);
        verifyNoInteraction();
    }

    private boolean setOK(String setToCheck, String set, Set<String> excludedSets) {
        if (setToCheck == null) {
            return false;
        }

        if ((setToCheck.equals(set) || set == null)
                && (excludedSets == null ||
                (excludedSets != null && !excludedSets.contains(setToCheck)))) {
            return true;
        }
        return false;
    }

    private boolean dateOK(Date dateToCheck, Date from, Date until) {
        if (dateToCheck == null) {
            return false;
        }
        if ((from == null || dateToCheck.equals(from)
                || dateToCheck.after(from))
                &&
                (until == null || dateToCheck.equals(until)
                        || dateToCheck.before(until))) {
            return true;
        }
        return false;
    }

    private void initHeaders(OAIPMHHarvestingDetails details) {
        headers = new HashSet<>();

        if (setOK(SET1, details.getSet(), details.getExcludedSets())
                && dateOK(DATE1, details.getDateFrom(), details.getDateUntil())) {
            headers.add(new Header().withIdentifier(ID1).withSetSpec(SET1).withDatestamp(DATE1));
        }
        if (setOK(SET2, details.getSet(), details.getExcludedSets())
                && dateOK(DATE2, details.getDateFrom(), details.getDateUntil())) {
            headers.add(new Header().withIdentifier(ID2).withSetSpec(SET2).withDatestamp(DATE2));
        }

        iterator = headers.iterator();
    }

    private void mockSource(Iterator<Header> headerIterator) {
        try {
            when(sourceProvider.provide(Mockito.anyString())).thenReturn(source);
            when(source.listIdentifiers(Mockito.any(ListIdentifiersParameters.class))).thenReturn(headerIterator);
        } catch (BadArgumentException e) {
            // nothing to report
        }
    }
}
