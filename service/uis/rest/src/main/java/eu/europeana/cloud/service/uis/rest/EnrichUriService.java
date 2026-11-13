package eu.europeana.cloud.service.uis.rest;

import eu.europeana.cloud.common.model.DataProvider;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpRequest;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Utility class that inserts absolute uris into classes that will be used as REST responses.
 */
public final class EnrichUriService {

  private final URI UIS_EXTERNAL_URI;

  public EnrichUriService(String uisExternalUri) {
    try {
      this.UIS_EXTERNAL_URI = new URI(uisExternalUri);
    } catch (URISyntaxException e) {
      throw new RuntimeException("Invalid MCS base URI: " + uisExternalUri, e);
    }
  }

  void enrich(HttpServletRequest httpServletRequest, DataProvider provider) {
    HttpRequest httpRequest = new ServletServerHttpRequest(httpServletRequest);

    UriComponentsBuilder builder = UriComponentsBuilder.fromUri(httpRequest.getURI());

    if (UIS_EXTERNAL_URI != null) {
      builder.scheme(UIS_EXTERNAL_URI.getScheme())
              .host(UIS_EXTERNAL_URI.getHost())
              .port(UIS_EXTERNAL_URI.getPort())
              .userInfo(UIS_EXTERNAL_URI.getUserInfo());
    }

    URI newUri = builder
            .pathSegment(provider.getId())
            .build()
            .toUri();

    provider.setUri(newUri);
  }
}
