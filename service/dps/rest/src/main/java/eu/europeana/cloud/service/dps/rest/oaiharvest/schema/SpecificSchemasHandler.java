package eu.europeana.cloud.service.dps.rest.oaiharvest.schema;

import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.rest.oaiharvest.OAIItem;

import java.util.Set;

/**
 * Created by Tarek on 7/5/2017.
 */
public class SpecificSchemasHandler extends SchemaHandler {

    /**
     * return the task specific schemas after excluding the excluded schemas
     */
    @Override
    public Set<String> getSchemas(OAIItem oaiItem) {
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = oaiItem.getSourceDetails();
        Set<String> schemas = oaipmhHarvestingDetails.getSchemas();
        Set<String> excludedSchemas = oaipmhHarvestingDetails.getExcludedSchemas();
        if (excludedSchemas != null) {
            schemas.removeAll(excludedSchemas);
        }
        return schemas;
    }
}
