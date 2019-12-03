package eu.europeana.cloud.service.dps.rest.oaiharvest.schema;

import eu.europeana.cloud.service.dps.rest.oaiharvest.OAIItem;

import java.util.Set;

/**
 * Created by Tarek on 4/30/2018.
 */
public abstract class SchemaHandler {
    public abstract Set<String> getSchemas (OAIItem oaiItem);
}
