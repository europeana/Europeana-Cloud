package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.schemas;

import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.Splitter;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Tarek on 7/5/2017.
 */
public class SpecificSchemasSplitter extends SchemasSplitter {
    SpecificSchemasSplitter(Splitter splitter) {
        super(splitter);
    }

    /**
     * use the task specific schemas and iterate over them to emit tuple per each schema
     */
    public void split() {
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = splitter.getStormTaskTuple().getSourceDetails();
        Set<String> schemas = oaipmhHarvestingDetails.getSchemas();
        Set<String> excludedSchemas = oaipmhHarvestingDetails.getExcludedSchemas();
        if (excludedSchemas != null)
            schemas.removeAll(excludedSchemas);
        for (String schema : schemas) {
            splitter.separateSchemaBySet(schema, oaipmhHarvestingDetails.getSets(), oaipmhHarvestingDetails.getDateFrom(), oaipmhHarvestingDetails.getDateUntil());
        }
    }
}
