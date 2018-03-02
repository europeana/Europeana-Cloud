package eu.europeana.cloud.service.uis.rest;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.common.model.DataProvider;

import javax.ws.rs.core.UriInfo;
import java.net.URI;

import static eu.europeana.cloud.common.web.ParamConstants.P_PROVIDER;

/**
 * Utility class that inserts absolute uris into classes that will be used as
 * REST responses.
 */
final class EnrichUriUtil {

    static void enrich(UriInfo uriInfo, DataProvider provider) {
        URI providerUri = uriInfo.getBaseUriBuilder().path(DataProviderResource.class)
                .buildFromMap(ImmutableMap.of(P_PROVIDER, provider.getId()));
        provider.setUri(uriInfo.resolve(providerUri));
    }

    private EnrichUriUtil() {
    }
}
