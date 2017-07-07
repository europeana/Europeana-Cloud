import com.lyncode.xoai.model.oaipmh.Header;
import com.lyncode.xoai.serviceprovider.ServiceProvider;
import com.lyncode.xoai.serviceprovider.exceptions.BadArgumentException;
import com.lyncode.xoai.serviceprovider.parameters.ListIdentifiersParameters;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.IdentifiersHarvestingBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

import java.util.*;

import static eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.IdentifiersHarvestingBolt.getTestInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class IdentifiersHarvestingBoltTest {
    private IdentifiersHarvestingBolt instance;
    private OutputCollector oc;
    private ServiceProvider source;
    private final int TASK_ID = 1;
    private final String TASK_NAME = "TASK_NAME";
    private final String OAI_URL = "http://lib.psnc.pl/dlibra/oai-pmh-repository.xml";
    private final String SCHEMA = "oai_dc";
    private final String SET1 = "set1";
    private final String SET2 = "set2";
    private final String ID1 = "oai:localhost:1";
    private final String ID2 = "oai:localhost:2";
    private Set<Header> headers;
    private Iterator<Header> iterator;

    @Before
    public void init() {
        oc = mock(OutputCollector.class);
        source = mock(ServiceProvider.class);
        instance = getTestInstance(oc, source);
    }

    @Ignore
    @Test
    public void testSimpleHarvesting() {
        //given
        instance = getTestInstance(oc, null); // overwrite the instance from init where mock source is used
        OAIPMHHarvestingDetails sourceDetails = new OAIPMHHarvestingDetails(OAI_URL, SCHEMA);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, null, null, new HashMap<String, String>(),new Revision(), sourceDetails);
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);
        //when
        instance.execute(tuple);
        //then
        verify(oc, atLeast(1)).emit(any(Tuple.class), captor.capture());
        assertThat(captor.getAllValues().size(), greaterThanOrEqualTo(1));
        verifyNoMoreInteractions(oc);
    }

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    @Test
    public void testURLInvalid() {
        //given
        OAIPMHHarvestingDetails sourceDetails = new OAIPMHHarvestingDetails(null, SCHEMA);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, null, null, new HashMap<String, String>(),new Revision(), sourceDetails);
        //when
        instance.execute(tuple);
        //then
        verify(oc, atMost(0)).emit(any(Tuple.class), captor.capture());
    }

    @Test
    public void testSchemaInvalid() {
        //given
        OAIPMHHarvestingDetails sourceDetails = new OAIPMHHarvestingDetails(OAI_URL, null);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, null, null, new HashMap<String, String>(),new Revision(), sourceDetails);
        //when
        instance.execute(tuple);
        //then
        verify(oc, atMost(0)).emit(any(Tuple.class), captor.capture());
    }


    @Test
    public void testDatesInvalid() {
        //given
        OAIPMHHarvestingDetails sourceDetails = new OAIPMHHarvestingDetails(OAI_URL, SCHEMA);
        Date from = new Date();
        try {
            Thread.sleep(5);
        } catch (InterruptedException e) {
            // nothing to report
        }
        Date until = new Date();
        sourceDetails.setDateUntil(from);
        sourceDetails.setDateFrom(until);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, null, null, new HashMap<String, String>(),new Revision(), sourceDetails);
        //when
        instance.execute(tuple);
        //then
        verify(oc, atMost(0)).emit(any(Tuple.class), captor.capture());
    }

    @Test
    public void testHarvestingAll() {
        //given
        initHeaders();
        try {
            when(source.listIdentifiers((ListIdentifiersParameters) anyObject())).thenReturn(iterator);
        } catch (BadArgumentException e) {
            // nothing to report
        }
        OAIPMHHarvestingDetails sourceDetails = new OAIPMHHarvestingDetails(OAI_URL, SCHEMA);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, null, null, new HashMap<String, String>(),new Revision(), sourceDetails);
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);
        //when
        instance.execute(tuple);
        //then
        verify(oc, times(2)).emit(any(Tuple.class), captor.capture());

        List<Values> values = captor.getAllValues();
        assertThat(values.size(), is(2));

        Set<String> identifiers = new HashSet<>();
        identifiers.add(((HashMap<String, String>)values.get(0).get(4)).get(PluginParameterKeys.OAI_IDENTIFIER));
        identifiers.add(((HashMap<String, String>)values.get(1).get(4)).get(PluginParameterKeys.OAI_IDENTIFIER));
        assertTrue(identifiers.contains(ID1));
        assertTrue(identifiers.contains(ID2));

        verifyNoMoreInteractions(oc);
    }

    @Test
    public void testSimpleHarvestingWhenExcludedSetSpecified() {
        //given
        initHeaders();
        try {
            when(source.listIdentifiers((ListIdentifiersParameters) anyObject())).thenReturn(iterator);
        } catch (BadArgumentException e) {
            // nothing to report
        }
        OAIPMHHarvestingDetails sourceDetails = new OAIPMHHarvestingDetails(OAI_URL, SCHEMA);
        Set<String> sets = new HashSet<>();
        sets.add(SET1);
        sourceDetails.setExcludedSets(sets);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, null, null, new HashMap<String, String>(),new Revision(), sourceDetails);
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);
        //when
        instance.execute(tuple);
        //then
        verify(oc, times(1)).emit(any(Tuple.class), captor.capture());

        List<Values> values = captor.getAllValues();
        assertThat(values.size(), is(1));
        assertThat(((HashMap<String, String>)values.get(0).get(4)).get(PluginParameterKeys.OAI_IDENTIFIER), is(ID2));

        verifyNoMoreInteractions(oc);
    }

    private void initHeaders() {
        headers = new HashSet<>();

        headers.add(new Header().withIdentifier(ID1).withSetSpec(SET1));
        headers.add(new Header().withIdentifier(ID2).withSetSpec(SET2));

        iterator = headers.iterator();
    }
}
