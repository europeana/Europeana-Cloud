package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout.schema;

import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.oaipmh.Harvester;
import eu.europeana.cloud.service.dps.oaipmh.HarvesterException;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import java.util.HashSet;
import org.junit.Test;

/**
 * Created by Tarek on 5/4/2018.
 */
public class AllSchemasHandlerTest {

    @Test
    public void testGetSchemas() throws HarvesterException {

        // Create harvester and handler
        final Harvester harvester = mock(Harvester.class);
        final AllSchemasHandler allSchemasHandler = new AllSchemasHandler(harvester);

        // Create tuple with file URL but no exclusions.
        final StormTaskTuple stormTaskTuple = new StormTaskTuple();
        final OAIPMHHarvestingDetails oaipmhHarvestingDetails = new OAIPMHHarvestingDetails();
        stormTaskTuple.setSourceDetails(oaipmhHarvestingDetails);
        final String fileUrl = "file url";
        stormTaskTuple.setFileUrl(fileUrl);

        // Check without exclusions.
        final HashSet<String> resultNoExclusions = new HashSet<>();
        when(harvester.getSchemas(fileUrl, null)).thenReturn(resultNoExclusions);
        assertSame(resultNoExclusions, allSchemasHandler.getSchemas(stormTaskTuple));

        // Add exclusions to tuple and test with exclusions.
        final HashSet<String> exclusions = new HashSet<>();
        oaipmhHarvestingDetails.setExcludedSchemas(exclusions);
        final HashSet<String> resultWithExclusions = new HashSet<>();
        when(harvester.getSchemas(eq(fileUrl), same(exclusions))).thenReturn(resultWithExclusions);
        assertSame(resultWithExclusions, allSchemasHandler.getSchemas(stormTaskTuple));
    }
}
