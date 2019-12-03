package eu.europeana.cloud.service.dps.rest.oaiharvest.schema;


import eu.europeana.cloud.service.dps.rest.oaiharvest.OAIItem;

import java.util.Set;

/**
 * Created by Tarek on 4/30/2018.
 */
public class SchemaFactory {
    public static SchemaHandler getSchemaHandler(OAIItem oaiItem) {
        Set<String> schemas = oaiItem.getSourceDetails().getSchemas();
        if (schemas == null || schemas.isEmpty())
            return new AllSchemasHandler();
        else
            return new SpecificSchemasHandler();
    }
}
