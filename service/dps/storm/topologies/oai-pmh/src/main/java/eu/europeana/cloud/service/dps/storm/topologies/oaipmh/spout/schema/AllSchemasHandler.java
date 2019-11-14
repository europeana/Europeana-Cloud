package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout.schema;

import eu.europeana.cloud.service.dps.oaipmh.Harvester;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import java.util.Set;

/**
 * Created by Tarek on 4/30/2018.
 */
public class AllSchemasHandler extends SchemaHandler {

    private final Harvester harvester;

    AllSchemasHandler(Harvester harvester) {
        this.harvester = harvester;
    }

    public AllSchemasHandler(int numberOfRetries, int timeBetweenRetries) {
        this(new Harvester(numberOfRetries, timeBetweenRetries));
    }

    /**
     * List all the resource schemas
     */
    @Override
    public Set<String> getSchemas(StormTaskTuple stormTaskTuple) {
        final String fileUrl = stormTaskTuple.getFileUrl();
        final Set<String> excludedSchemas = stormTaskTuple.getSourceDetails().getExcludedSchemas();
        return harvester.getSchemas(fileUrl, excludedSchemas);
    }
}
