package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout.schema;

import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Tarek on 5/4/2018.
 */
public class SchemaFactoryTest {
    private final static String SCHEMA = "SCHEMA";
    private StormTaskTuple stormTaskTuple;

    @Before
    public void init() {
        stormTaskTuple = new StormTaskTuple();
    }

    @Test
    public void shouldReturnAllSchemaHandler() {
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = new OAIPMHHarvestingDetails();
        stormTaskTuple.setSourceDetails(oaipmhHarvestingDetails);
        SchemaHandler schemaHandler = SchemaFactory.getSchemaHandler(stormTaskTuple, 3, 1000);
        assertTrue(schemaHandler instanceof AllSchemasHandler);
    }

    @Test
    public void shouldReturnSpecificSchemaHandler() {
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = new OAIPMHHarvestingDetails(SCHEMA);
        stormTaskTuple.setSourceDetails(oaipmhHarvestingDetails);
        SchemaHandler schemaHandler = SchemaFactory.getSchemaHandler(stormTaskTuple, 3, 1000);
        assertTrue(schemaHandler instanceof SpecificSchemasHandler);
    }

}