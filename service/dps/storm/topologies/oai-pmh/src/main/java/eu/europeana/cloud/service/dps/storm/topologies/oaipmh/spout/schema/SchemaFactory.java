package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout.schema;

import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import java.util.Set;

/**
 * Created by Tarek on 4/30/2018.
 */
public class SchemaFactory {
    public static SchemaHandler getSchemaHandler(StormTaskTuple stormTaskTuple) {
        Set<String> schemas = stormTaskTuple.getSourceDetails().getSchemas();
        if (schemas == null || schemas.isEmpty())
            return new AllSchemasHandler();
        else
            return new SpecificSchemasHandler();
    }
}
