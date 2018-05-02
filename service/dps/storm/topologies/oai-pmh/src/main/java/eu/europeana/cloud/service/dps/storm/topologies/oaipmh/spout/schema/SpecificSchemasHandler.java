package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout.schema;

import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.common.OAIHelper;

import java.util.List;
import java.util.Set;

/**
 * Created by Tarek on 7/5/2017.
 */
public class SpecificSchemasHandler extends SchemaHandler {

    /**
     * return the task specific schemas after excluding the excluded schemas
     */
    public Set<String> getSchemas(StormTaskTuple stormTaskTuple) {
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = stormTaskTuple.getSourceDetails();
        OAIHelper oaiHelper = new OAIHelper(stormTaskTuple.getFileUrl());
        Set<String> schemas = oaipmhHarvestingDetails.getSchemas();
        Set<String> excludedSchemas = oaipmhHarvestingDetails.getExcludedSchemas();
        oaipmhHarvestingDetails.setGranularity(oaiHelper.getGranularity().toString());
        if (excludedSchemas != null) {
            schemas.removeAll(excludedSchemas);
        }
        return schemas;
    }
}
