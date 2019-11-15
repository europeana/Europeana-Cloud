package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout.schema;

import eu.europeana.cloud.service.dps.oaipmh.HarvesterException;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;

import java.util.Set;

/**
 * Created by Tarek on 4/30/2018.
 */
public interface SchemaHandler {
    Set<String> getSchemas (StormTaskTuple stormTaskTuple) throws HarvesterException;
}
