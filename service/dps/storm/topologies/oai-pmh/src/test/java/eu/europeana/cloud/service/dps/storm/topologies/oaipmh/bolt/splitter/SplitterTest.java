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

import static org.junit.Assert.assertEquals;
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

    protected StormTaskTuple stormTaskTuple;
    private OutputCollector outputCollector;
    private Set<String> sets;
    private OAIHelper oaiHelper;
    private OAIPMHHarvestingDetails oaipmhHarvestingDetails;
    private Tuple inputTuple;
    private Splitter splitter;

    @Before
    public void init() {
        sets = new HashSet<>();
        oaiHelper = mock(OAIHelper.class);
        outputCollector = mock(OutputCollector.class);
        oaipmhHarvestingDetails = mock(OAIPMHHarvestingDetails.class);
        initTestScenarioWithTwoSchemas();
        stormTaskTuple = new StormTaskTuple(TASK_ID, TASK_NAME, null, null, new HashMap<String, String>(), new Revision(), oaipmhHarvestingDetails);
        splitter = new Splitter(stormTaskTuple, inputTuple, outputCollector, oaiHelper);
    }

    @Test
    public void testSplitWithTwoSchemasAndNullSets() {
        splitter.splitBySchema();
        verify(outputCollector, times(2)).emit(any(Tuple.class), anyList());
    }

    @Test
    public void testSplitWithTwoSchemasAndEmptySets() {
        when(oaipmhHarvestingDetails.getSets()).thenReturn(sets);
        splitter.splitBySchema();
        verify(outputCollector, times(2)).emit(any(Tuple.class), anyList());
    }


    @Test
    public void testSplitWithTwoSchemasAndTwoSets() {
        includeTwoSetsToTheTask();
        splitter.splitBySchema();
        verify(outputCollector, times(4)).emit(any(Tuple.class), anyList());
    }

    @Test
    public void testSplitWithTwoSchemasAndTwoSetsWithThreeMonthsRange() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        Date start = sdf.parse("15/1/2012");
        Date end = sdf.parse("1/4/2012");
        when(oaipmhHarvestingDetails.getDateFrom()).thenReturn(start);
        when(oaipmhHarvestingDetails.getDateUntil()).thenReturn(end);
        includeTwoSetsToTheTask();
        splitter.splitBySchema();
        verify(outputCollector, times(12)).emit(any(Tuple.class), captor.capture());
        assertEquals(captor.getAllValues().size(), 12);
    }


    @Captor
    ArgumentCaptor<Values> captor = ArgumentCaptor.forClass(Values.class);

    private void includeTwoSetsToTheTask() {
        sets.add("SET1");
        sets.add("SET2");
        when(oaipmhHarvestingDetails.getSets()).thenReturn(sets);
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