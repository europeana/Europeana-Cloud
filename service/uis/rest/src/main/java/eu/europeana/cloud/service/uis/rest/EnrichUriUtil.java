package eu.europeana.cloud.service.uis.rest;

import eu.europeana.cloud.common.model.DataProvider;

import javax.servlet.http.HttpServletRequest;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Utility class that inserts absolute uris into classes that will be used as
 * REST responses.
 */
final class EnrichUriUtil {

    private EnrichUriUtil() {
    }

    static void enrich(HttpServletRequest httpServletRequest, DataProvider provider) throws URISyntaxException {
        provider.setUri(new URI(httpServletRequest.getRequestURL() + "/" + provider.getId()));
    }
}
