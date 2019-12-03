package eu.europeana.cloud.service.dps.rest.oaiharvest.schema;

import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.rest.oaiharvest.OAIItem;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by Tarek on 5/4/2018.
 */
public class SchemaFactoryTest {
    private final static String SCHEMA = "SCHEMA";
    private OAIItem oaiItem;

    @Before
    public void init() {
        oaiItem = new OAIItem();
    }

    @Test
    public void shouldReturnAllSchemaHandler() {
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = new OAIPMHHarvestingDetails();
        oaiItem.setSourceDetails(oaipmhHarvestingDetails);
        SchemaHandler schemaHandler = SchemaFactory.getSchemaHandler(oaiItem);
        assertTrue(schemaHandler instanceof AllSchemasHandler);
    }

    @Test
    public void shouldReturnSpecificSchemaHandler() {
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = new OAIPMHHarvestingDetails(SCHEMA);
        oaiItem.setSourceDetails(oaipmhHarvestingDetails);
        SchemaHandler schemaHandler = SchemaFactory.getSchemaHandler(oaiItem);
        assertTrue(schemaHandler instanceof SpecificSchemasHandler);
    }

}