package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.schemas;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.Splitter;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.common.OAIHelper;
import org.apache.storm.task.OutputCollector;
import org.dspace.xoai.model.oaipmh.Granularity;
import org.junit.Before;
import org.junit.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Matchers.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

/**
 * Created by Tarek on 7/7/2017.
 */
public class SpecificSchemasSplitterTest {
    private final String SCHEMA1 = "oai_dc";
    private final String SCHEMA2 = "oai_qdc ";
    private final String TASK_NAME = "TASK_NAME";
    private final long TASK_ID = 1;
    protected StormTaskTuple stormTaskTuple;
    private OutputCollector outputCollector;
    private SpecificSchemasSplitter specificSchemasSplitter;
    private Splitter splitter;
    private Set<String> schemas;
    private OAIPMHHarvestingDetails oaipmhHarvestingDetails;
    private OAIHelper oaiHelper;

    @Before
    public void init() {
        oaiHelper = mock(OAIHelper.class);
        outputCollector = mock(OutputCollector.class);
        splitter = mock(Splitter.class);
        oaipmhHarvestingDetails = mock(OAIPMHHarvestingDetails.class);
        schemas = new HashSet<>();
        schemas.add(SCHEMA1);
        schemas.add(SCHEMA2);
        stormTaskTuple = new StormTaskTuple(TASK_ID, TASK_NAME, null, null, new HashMap<String, String>(), new Revision(), oaipmhHarvestingDetails);

        initTestScenario();
    }

    @Test
    public void testSplitForSpecificSchemasWithEmptyExcludedSchemas() {
        when(oaipmhHarvestingDetails.getExcludedSchemas()).thenReturn(new HashSet<String>());//empty excluded schemas
        specificSchemasSplitter = new SpecificSchemasSplitter(splitter);
        specificSchemasSplitter.split();
        verify(splitter, times(2)).separateSchemaBySet(anyString(), anySet(), any(Date.class), any(Date.class));
    }

    @Test
    public void testSplitForSpecificSchemasWithNullExcludedSchemas() {
        when(oaipmhHarvestingDetails.getExcludedSchemas()).thenReturn(null);//null excludedSchemas
        specificSchemasSplitter = new SpecificSchemasSplitter(splitter);
        specificSchemasSplitter.split();
        verify(splitter, times(2)).separateSchemaBySet(anyString(), anySet(), any(Date.class), any(Date.class));
    }

    @Test
    public void testSplitForSpecificSchemasWithProvidedExcludedSchemas() {
        Set<String> excludedSchemas = new HashSet<>();
        excludedSchemas.add(SCHEMA1);
        when(oaipmhHarvestingDetails.getExcludedSchemas()).thenReturn(excludedSchemas);
        specificSchemasSplitter = new SpecificSchemasSplitter(splitter);
        specificSchemasSplitter.split();
        verify(splitter, times(1)).separateSchemaBySet(anyString(), anySet(), any(Date.class), any(Date.class));
    }

    private void initTestScenario() {
//        when(oaiHelper.getGranularity()).thenReturn(Granularity.Second);
//        when(splitter.getOaiHelper()).thenReturn(oaiHelper);
        when(splitter.getGranularity()).thenReturn(Granularity.Second);
        when(outputCollector.emit(anyList())).thenReturn(null);
        when(splitter.getStormTaskTuple()).thenReturn(stormTaskTuple);
        when(oaipmhHarvestingDetails.getSchemas()).thenReturn(schemas);
    }
}

