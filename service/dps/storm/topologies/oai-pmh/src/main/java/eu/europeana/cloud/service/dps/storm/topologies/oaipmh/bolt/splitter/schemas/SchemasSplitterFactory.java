package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.schemas;

import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.Splitter;

import java.util.HashSet;
import java.util.Set;

/**
 * return the appropriate splitter based on the provided schemas
 */
public class SchemasSplitterFactory {

    public static SchemasSplitter getTaskSplitter(Splitter splitter) {
        Set<String> schemas = splitter.getStormTaskTuple().getSourceDetails().getSchemas();
        if (schemas == null || schemas.isEmpty())
            return new AllSchemasSplitter(splitter);
        else
            return new SpecificSchemasSplitter(splitter);
    }
}
