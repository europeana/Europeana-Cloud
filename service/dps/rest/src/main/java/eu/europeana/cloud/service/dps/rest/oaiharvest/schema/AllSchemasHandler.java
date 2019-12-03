package eu.europeana.cloud.service.dps.rest.oaiharvest.schema;

import eu.europeana.cloud.service.dps.OAIPMHHarvestingDetails;
import eu.europeana.cloud.service.dps.rest.oaiharvest.OAIHelper;
import eu.europeana.cloud.service.dps.rest.oaiharvest.OAIItem;
import org.dspace.xoai.model.oaipmh.MetadataFormat;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by Tarek on 4/30/2018.
 */
public class AllSchemasHandler extends SchemaHandler {
    /**
     * List all the resource schemas
     */
    public Set<String> getSchemas(OAIItem oaiItem) {
        OAIHelper oaiHelper = new OAIHelper(oaiItem.getFileUrl());
        Iterator<MetadataFormat> metadataFormatIterator = oaiHelper.listSchemas();
        OAIPMHHarvestingDetails oaipmhHarvestingDetails = oaiItem.getSourceDetails();
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
