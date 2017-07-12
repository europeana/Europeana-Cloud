package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter;

import com.lyncode.xoai.model.oaipmh.MetadataFormat;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.common.OAIHelper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;


/**
 * Created by Tarek on 7/10/2017.
 */
public class SplitterTest {
    private final String SCHEMA1 = "oai_dc";
    private final String SCHEMA2 = "oai_qdc ";
    private final String TASK_NAME = "TASK_NAME";
    private final long TASK_ID = 1;
    private final long INTERVAL = 2592000000l;

    protected StormTaskTuple stormTaskTuple;
    private OutputCollector outputCollector;
    private OAIHelper oaiHelper;
    private OAIPMHHarvestingDetails oaipmhHarvestingDetails;
    private Tuple inputTuple;
    private Splitter splitter;

    @Before
    public void init() {
        oaiHelper = mock(OAIHelper.class);
        outputCollector = mock(OutputCollector.class);
        oaipmhHarvestingDetails = new OAIPMHHarvestingDetails();
        initTestScenarioWithTwoSchemas();
        stormTaskTuple = new StormTaskTuple(TASK_ID, TASK_NAME, null, null, new HashMap<String, String>(), new Revision(), oaipmhHarvestingDetails);
        splitter = new Splitter(stormTaskTuple, inputTuple, outputCollector, oaiHelper, INTERVAL);
    }

    @Test
    public void testSplitWithTwoSchemasAndNullSets() {
        splitter.splitBySchema();
        verify(outputCollector, times(2)).emit(any(Tuple.class), anyList());
    }

    @Test
    public void testSplitWithTwoSchemasAndEmptySets() {
        oaipmhHarvestingDetails.setSets(new HashSet<String>());
        splitter.splitBySchema();
        verify(outputCollector, times(2)).emit(any(Tuple.class), anyList());
    }


    @Test
    public void testSplitWithTwoSchemasAndTwoSets() {
        oaipmhHarvestingDetails.setSets(buildTwoItemsSet());
        splitter.splitBySchema();
        verify(outputCollector, times(4)).emit(any(Tuple.class), captor.capture());
        assertEquals(captor.getAllValues().size(), 4);
        assertTupleValues(30);
    }

    @Test
    public void testSplitWithTwoSchemasAndTwoSetsWithLessThanMonthRange() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        Date start = sdf.parse("15/3/2012");
        Date end = sdf.parse("1/4/2012");
        oaipmhHarvestingDetails.setDateFrom(start);
        oaipmhHarvestingDetails.setDateUntil(end);
        oaipmhHarvestingDetails.setSets(buildTwoItemsSet());
        splitter.splitBySchema();
        verify(outputCollector, times(4)).emit(any(Tuple.class), captor.capture());
        assertEquals(captor.getAllValues().size(), 4);
        assertTupleValues(16);
    }


    @Test
    public void testSplitWithTwoSchemasAndTwoSetsWithProvidedInterval() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        Date start = sdf.parse("1/3/2012");
        Date end = sdf.parse("1/4/2012");
        oaipmhHarvestingDetails.setDateFrom(start);
        oaipmhHarvestingDetails.setDateUntil(end);
        oaipmhHarvestingDetails.setSets(buildTwoItemsSet());
        stormTaskTuple.getParameters().put(PluginParameterKeys.INTERVAL, "86400000");//one day
        splitter.splitBySchema();
        verify(outputCollector, times(124)).emit(any(Tuple.class), captor.capture());//2*2*31
        assertEquals(captor.getAllValues().size(), 124);
        assertTupleValues(1);
    }


    @Test
    public void testSplitWithTwoSchemasAndTwoSetsWithThreeMonthsRange() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        Date start = sdf.parse("15/1/2012");
        Date end = sdf.parse("1/4/2012");
        oaipmhHarvestingDetails.setDateFrom(start);
        oaipmhHarvestingDetails.setDateUntil(end);
        oaipmhHarvestingDetails.setSets(buildTwoItemsSet());
        splitter.splitBySchema();
        verify(outputCollector, times(12)).emit(any(Tuple.class), captor.capture());
        assertEquals(captor.getAllValues().size(), 12);
        assertTupleValues(30);
    }

    private void assertTupleValues(int maxIntervalTimeInDay) {
        for (Values values : captor.getAllValues()) {
            assertTrue(values.get(6) instanceof OAIPMHHarvestingDetails);
            OAIPMHHarvestingDetails oaipmhHarvestingDetails = (OAIPMHHarvestingDetails) values.get(6);
            assertEquals((oaipmhHarvestingDetails.getSchemas().size()), 1);
            assertEquals((oaipmhHarvestingDetails.getSets().size()), 1);
            long diffInDays = TimeUnit.DAYS.convert(oaipmhHarvestingDetails.getDateUntil().getTime() - oaipmhHarvestingDetails.getDateFrom().getTime(), TimeUnit.MILLISECONDS);
            assertTrue(diffInDays <= maxIntervalTimeInDay);
        }
    }

    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    private Set<String> buildTwoItemsSet() {
        Set<String> sets = new HashSet<>();
        sets.add("SET1");
        sets.add("SET2");
        return sets;
    }

    private void initTestScenarioWithTwoSchemas() {
        when(outputCollector.emit(anyList())).thenReturn(null);
        Iterator<MetadataFormat> metadataFormatIterator = mock(Iterator.class);
        when(metadataFormatIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        MetadataFormat metadataFormat = mock(MetadataFormat.class);
        when(metadataFormatIterator.next()).thenReturn(metadataFormat);
        when(metadataFormat.getMetadataPrefix()).thenReturn(SCHEMA1).thenReturn(SCHEMA2);
        when(oaiHelper.listSchemas()).thenReturn(metadataFormatIterator);
        when(oaiHelper.getEarlierDate()).thenReturn(new Date());

    }

}