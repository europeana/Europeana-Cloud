package eu.europeana.cloud.common.filter;

import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientRequestFilter;
import jakarta.ws.rs.core.HttpHeaders;

import java.io.IOException;

/**
 * Client request filter which will add provided header value as a authorization header to request
 */
public class ECloudBasicAuthFilter implements ClientRequestFilter {

  private String headerValue;

  public ECloudBasicAuthFilter(String headerValue) {
    this.headerValue = headerValue;
  }

  @Override
  public void filter(ClientRequestContext requestContext) throws IOException {
    requestContext.getHeaders().remove(HttpHeaders.AUTHORIZATION);
    if (!requestContext.getHeaders().containsKey(HttpHeaders.AUTHORIZATION)) {
      requestContext.getHeaders().add(HttpHeaders.AUTHORIZATION, headerValue);
    }
  }
}
