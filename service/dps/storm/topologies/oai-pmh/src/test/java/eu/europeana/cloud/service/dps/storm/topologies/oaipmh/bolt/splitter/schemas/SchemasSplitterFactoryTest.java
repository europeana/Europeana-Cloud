package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.schemas;

import com.lyncode.xoai.model.oaipmh.Granularity;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.Splitter;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.common.OAIHelper;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by Tarek on 7/7/2017.
 */
public class SchemasSplitterFactoryTest {
    private final String SCHEMA = "oai_dc";
    private final String TASK_NAME = "TASK_NAME";
    private final long TASK_ID = 1;
    private final long INTERVAL = 2592000000l;

    private OAIHelper oaiHelper;
    protected StormTaskTuple stormTaskTuple;
    private Splitter splitter;

    @Before
    public void init() {
        oaiHelper = mock(OAIHelper.class);
        stormTaskTuple = new StormTaskTuple(TASK_ID, TASK_NAME, null, null, new HashMap<String, String>(), new Revision());
        splitter = new Splitter(stormTaskTuple, null, null, oaiHelper, INTERVAL);
        prepareMocks();
    }

    private void prepareMocks() {
        when(oaiHelper.getGranularity()).thenReturn(Granularity.Second);
    }

    @Test
    public void getSpecificSchemasSplitter() {
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = new OAIPMHHarvestingDetails(SCHEMA);
        stormTaskTuple.setSourceDetails(oaipmhHarvestingDetails);
        SchemasSplitter taskSplitter = SchemasSplitterFactory.getTaskSplitter(splitter);
        assertTrue(taskSplitter instanceof SpecificSchemasSplitter);
    }

    @Test
    public void getAllSchemasSplitter() {
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = new OAIPMHHarvestingDetails();
        stormTaskTuple.setSourceDetails(oaipmhHarvestingDetails);
        SchemasSplitter taskSplitter = SchemasSplitterFactory.getTaskSplitter(splitter);
        assertTrue(taskSplitter instanceof AllSchemasSplitter);
    }

    @Test
    public void getAllSchemasSplitterWhenSchemasAreEmpty() {
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = new OAIPMHHarvestingDetails(new HashSet<String>(), null, null, null, null);
        stormTaskTuple.setSourceDetails(oaipmhHarvestingDetails);
        SchemasSplitter taskSplitter = SchemasSplitterFactory.getTaskSplitter(splitter);
        assertTrue(taskSplitter instanceof AllSchemasSplitter);
    }


}