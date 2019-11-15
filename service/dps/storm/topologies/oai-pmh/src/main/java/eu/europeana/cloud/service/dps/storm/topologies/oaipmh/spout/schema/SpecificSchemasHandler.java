package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout.schema;

import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import java.util.Set;

/**
 * Created by Tarek on 7/5/2017.
 */
public class SpecificSchemasHandler implements SchemaHandler {

    /**
     * return the task specific schemas after excluding the excluded schemas
     */
    @Override
    public Set<String> getSchemas(StormTaskTuple stormTaskTuple) {
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = stormTaskTuple.getSourceDetails();
        Set<String> schemas = oaipmhHarvestingDetails.getSchemas();
        Set<String> excludedSchemas = oaipmhHarvestingDetails.getExcludedSchemas();
        if (excludedSchemas != null) {
            schemas.removeAll(excludedSchemas);
        }
        return schemas;
    }
}
