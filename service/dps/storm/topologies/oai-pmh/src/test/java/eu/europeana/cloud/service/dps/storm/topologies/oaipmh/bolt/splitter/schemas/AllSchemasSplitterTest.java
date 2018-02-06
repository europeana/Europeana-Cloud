package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.schemas;

import org.dspace.xoai.model.oaipmh.MetadataFormat;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.Splitter;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.common.OAIHelper;
import org.apache.storm.task.OutputCollector;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.mockito.Matchers.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/**
 * Created by Tarek on 7/7/2017.
 */
public class AllSchemasSplitterTest {
    private final String SCHEMA1 = "oai_dc";
    private final String SCHEMA2 = "oai_qdc ";
    private final String TASK_NAME = "TASK_NAME";
    private final long TASK_ID = 1;

    protected StormTaskTuple stormTaskTuple;
    private OutputCollector outputCollector;
    private AllSchemasSplitter allSchemasSplitter;
    private OAIHelper oaiHelper;
    private Splitter splitter;
    private OAIPMHHarvestingDetails oaipmhHarvestingDetails;

    @Before
    public void init() {
        oaiHelper = mock(OAIHelper.class);
        outputCollector = mock(OutputCollector.class);
        splitter = mock(Splitter.class);
        oaipmhHarvestingDetails = new OAIPMHHarvestingDetails();
        stormTaskTuple = new StormTaskTuple(TASK_ID, TASK_NAME, null, null, new HashMap<String, String>(), new Revision(), oaipmhHarvestingDetails);

        initTestScenario();
    }

    @Test
    public void testSplitForAllSchemasWithNoExcludedSchemas()

    {
        allSchemasSplitter = new AllSchemasSplitter(splitter);
        allSchemasSplitter.split();
        verify(splitter, times(2)).separateSchemaBySet(anyString(), anySet(), any(Date.class), any(Date.class));

    }

    @Test
    public void testSplitForAllSchemasExcludedSchemas()

    {
        Set<String> excludedSchemas = new HashSet<>();
        excludedSchemas.add(SCHEMA1);
        oaipmhHarvestingDetails.setExcludedSchemas(excludedSchemas);
        allSchemasSplitter = new AllSchemasSplitter(splitter);
        allSchemasSplitter.split();
        verify(splitter, times(1)).separateSchemaBySet(anyString(), anySet(), any(Date.class), any(Date.class));

    }

    private void initTestScenario() {
        when(outputCollector.emit(anyList())).thenReturn(null);
        when(splitter.getOaiHelper()).thenReturn(oaiHelper);
        when(splitter.getStormTaskTuple()).thenReturn(stormTaskTuple);
        Iterator<MetadataFormat> metadataFormatIterator = mock(Iterator.class);
        when(metadataFormatIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        MetadataFormat metadataFormat = mock(MetadataFormat.class);
        when(metadataFormatIterator.next()).thenReturn(metadataFormat);
        when(metadataFormat.getMetadataPrefix()).thenReturn(SCHEMA1).thenReturn(SCHEMA2);
        when(oaiHelper.listSchemas()).thenReturn(metadataFormatIterator);
    }
}
