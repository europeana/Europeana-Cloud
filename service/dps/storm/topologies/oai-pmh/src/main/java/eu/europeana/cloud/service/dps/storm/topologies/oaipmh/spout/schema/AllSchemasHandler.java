package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout.schema;

import eu.europeana.cloud.service.dps.oaipmh.Harvester;
import eu.europeana.cloud.service.dps.oaipmh.HarvesterException;
import eu.europeana.cloud.service.dps.oaipmh.HarvesterFactory;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import java.util.Set;

/**
 * Created by Tarek on 4/30/2018.
 */
public class AllSchemasHandler implements SchemaHandler {

    private final Harvester harvester;

    AllSchemasHandler(Harvester harvester) {
        this.harvester = harvester;
    }

    public AllSchemasHandler(int numberOfRetries, int timeBetweenRetries) {
        this(HarvesterFactory.createHarvester(numberOfRetries, timeBetweenRetries));
    }

    /**
     * List all the resource schemas
     */
    @Override
    public Set<String> getSchemas(StormTaskTuple stormTaskTuple) throws HarvesterException {
        final String fileUrl = stormTaskTuple.getFileUrl();
        final Set<String> excludedSchemas = stormTaskTuple.getSourceDetails().getExcludedSchemas();
        return harvester.getSchemas(fileUrl, excludedSchemas);
    }
}
