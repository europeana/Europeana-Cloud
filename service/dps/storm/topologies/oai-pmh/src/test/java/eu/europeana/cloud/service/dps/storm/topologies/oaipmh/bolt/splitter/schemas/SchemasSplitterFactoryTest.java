package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.schemas;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.Splitter;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.common.OAIHelper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;

import static org.junit.Assert.*;

/**
 * Created by Tarek on 7/7/2017.
 */
public class SchemasSplitterFactoryTest {
    private final String SCHEMA = "oai_dc";
    private final String TASK_NAME = "TASK_NAME";
    private final long TASK_ID = 1;

    protected StormTaskTuple stormTaskTuple;
    private Splitter splitter;

    @Before
    public void init() {
        stormTaskTuple = new StormTaskTuple(TASK_ID, TASK_NAME, null, null, new HashMap<String, String>(), new Revision());
        splitter = new Splitter(stormTaskTuple, null, null, null);

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
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = new OAIPMHHarvestingDetails(new HashSet<String>(), null, null, null);
        stormTaskTuple.setSourceDetails(oaipmhHarvestingDetails);
        SchemasSplitter taskSplitter = SchemasSplitterFactory.getTaskSplitter(splitter);
        assertTrue(taskSplitter instanceof AllSchemasSplitter);
    }


}