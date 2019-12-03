package eu.europeana.cloud.service.dps.rest.oaiharvest.schema;

import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.rest.oaiharvest.OAIItem;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Created by Tarek on 5/4/2018.
 */
public class SpecificSchemasHandlerTest {

    public static final String EDM = "EDM";
    public static final String RDF = "RDF";
    public static final String OAI_DC = "OAI_DC";
    private OAIItem oaiItem;

    @Before
    public void init() throws Exception {
        oaiItem = new OAIItem();

    }

    @Test
    public void shouldReturnAllSchemasWithNoExcludedSchemas() throws Exception {
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = new OAIPMHHarvestingDetails();
        oaipmhHarvestingDetails.setSchemas(new HashSet<>(Arrays.asList(OAI_DC, RDF, EDM)));

        oaiItem.setSourceDetails(oaipmhHarvestingDetails);
        SpecificSchemasHandler specificSchemasHandler = new SpecificSchemasHandler();
        Set<String> schemas = specificSchemasHandler.getSchemas(oaiItem);
        assertNotNull(schemas);
        assertEquals(3, schemas.size());

    }

    @Test
    public void shouldReturnAllSchemasWithExcludedSchemas() throws Exception {
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = new OAIPMHHarvestingDetails();
        oaipmhHarvestingDetails.setSchemas(new HashSet<>(Arrays.asList(OAI_DC, RDF, EDM)));
        oaipmhHarvestingDetails.setExcludedSchemas(new HashSet<>(Arrays.asList(OAI_DC, RDF)));
        oaiItem.setSourceDetails(oaipmhHarvestingDetails);
        SpecificSchemasHandler specificSchemasHandler = new SpecificSchemasHandler();
        Set<String> schemas = specificSchemasHandler.getSchemas(oaiItem);
        assertNotNull(schemas);
        assertEquals(1, schemas.size());
        assertEquals(EDM, schemas.iterator().next());

    }

}