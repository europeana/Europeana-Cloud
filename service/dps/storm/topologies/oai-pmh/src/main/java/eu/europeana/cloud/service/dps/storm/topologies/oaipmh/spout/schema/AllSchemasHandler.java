package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout.schema;

import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.bolt.splitter.Splitter;
import eu.europeana.cloud.service.dps.storm.topologies.oaipmh.common.OAIHelper;
import org.dspace.xoai.model.oaipmh.MetadataFormat;

import java.util.*;

/**
 * Created by Tarek on 4/30/2018.
 */
public class AllSchemasHandler extends SchemaHandler {
    /**
     * List all the resource schemas
     */
    public Set<String> getSchemas(StormTaskTuple stormTaskTuple) {
        OAIHelper oaiHelper = new OAIHelper(stormTaskTuple.getFileUrl());
        Iterator<MetadataFormat> metadataFormatIterator = oaiHelper.listSchemas();
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = stormTaskTuple.getSourceDetails();
        Set<String> excludedSchemas = oaipmhHarvestingDetails.getExcludedSchemas();
        Set<String> schemas = new HashSet<>();
        while (metadataFormatIterator.hasNext()) {
            String schema = metadataFormatIterator.next().getMetadataPrefix();
            if (excludedSchemas == null || !excludedSchemas.contains(schema)) {
                schemas.add(schema);
            }
        }
        return schemas;

    }
}
