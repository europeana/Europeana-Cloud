package eu.europeana.cloud.service.uis.rest;

import eu.europeana.cloud.common.model.DataProvider;
import org.springframework.http.HttpRequest;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.web.util.UriComponentsBuilder;

import javax.servlet.http.HttpServletRequest;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Utility class that inserts absolute uris into classes that will be used as REST responses.
 */
final class EnrichUriUtil {

  private EnrichUriUtil() {
  }

  static void enrich(HttpServletRequest httpServletRequest, DataProvider provider) {
    HttpRequest httpRequest = new ServletServerHttpRequest(httpServletRequest);
    URI newUri = UriComponentsBuilder.fromHttpRequest(httpRequest).pathSegment(provider.getId()).build().toUri();
    provider.setUri(newUri);
  }
}
