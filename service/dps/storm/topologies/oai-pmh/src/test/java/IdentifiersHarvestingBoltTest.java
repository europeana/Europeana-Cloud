import com.lyncode.xoai.model.oaipmh.Header;
import com.lyncode.xoai.model.oaipmh.Identify;
import com.lyncode.xoai.model.oaipmh.ListIdentifiers;
import com.lyncode.xoai.serviceprovider.ServiceProvider;
import com.lyncode.xoai.serviceprovider.client.HttpOAIClient;
import com.lyncode.xoai.serviceprovider.client.OAIClient;
import com.lyncode.xoai.serviceprovider.exceptions.BadArgumentException;
import com.lyncode.xoai.serviceprovider.model.Context;
import com.lyncode.xoai.serviceprovider.parameters.ListIdentifiersParameters;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.OAIPMHSourceDetails;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.IdentifiersHarvestingBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;

import java.net.URISyntaxException;
import java.util.*;

import static eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.IdentifiersHarvestingBolt.getTestInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
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
        OAIPMHSourceDetails sourceDetails = new OAIPMHSourceDetails(OAI_URL, SCHEMA);
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
        OAIPMHSourceDetails sourceDetails = new OAIPMHSourceDetails(null, SCHEMA);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, null, null, new HashMap<String, String>(),new Revision(), sourceDetails);
        //when
        instance.execute(tuple);
        //then
        verify(oc, atMost(0)).emit(any(Tuple.class), captor.capture());
    }

    @Test
    public void testSchemaInvalid() {
        //given
        OAIPMHSourceDetails sourceDetails = new OAIPMHSourceDetails(OAI_URL, null);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, null, null, new HashMap<String, String>(),new Revision(), sourceDetails);
        //when
        instance.execute(tuple);
        //then
        verify(oc, atMost(0)).emit(any(Tuple.class), captor.capture());
    }

    @Test
    public void testSetsInvalid() {
        //given
        OAIPMHSourceDetails sourceDetails = new OAIPMHSourceDetails(OAI_URL, SCHEMA);
        Set<String> sets = new HashSet<>();
        sets.add(SET1);
        Set<String> excluded = new HashSet<>();
        excluded.add(SET2);
        sourceDetails.setSets(sets);
        sourceDetails.setExcludedSets(excluded);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, null, null, new HashMap<String, String>(),new Revision(), sourceDetails);
        //when
        instance.execute(tuple);
        //then
        verify(oc, atMost(0)).emit(any(Tuple.class), captor.capture());
    }


    @Test
    public void testDatesInvalid() {
        //given
        OAIPMHSourceDetails sourceDetails = new OAIPMHSourceDetails(OAI_URL, SCHEMA);
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
    public void testSimpleHarvestingWhenSetSpecified() {
        //given
        initHeaders();
        try {
            when(source.listIdentifiers((ListIdentifiersParameters) anyObject())).thenReturn(iterator);
        } catch (BadArgumentException e) {
            // nothing to report
        }
        OAIPMHSourceDetails sourceDetails = new OAIPMHSourceDetails(OAI_URL, SCHEMA);
        StormTaskTuple tuple = new StormTaskTuple(TASK_ID, TASK_NAME, null, null, new HashMap<String, String>(),new Revision(), sourceDetails);
        when(oc.emit(any(Tuple.class), anyList())).thenReturn(null);
        //when
        instance.execute(tuple);
        //then
        verify(oc, times(2)).emit(any(Tuple.class), captor.capture());
        assertThat(captor.getAllValues().size(), is(2));
        verifyNoMoreInteractions(oc);
    }

    private void initHeaders() {
        headers = new HashSet<>();

        headers.add(new Header().withIdentifier(ID1).withSetSpec(SET1));
        headers.add(new Header().withIdentifier(ID2).withSetSpec(SET2));

        iterator = headers.iterator();
    }
}
