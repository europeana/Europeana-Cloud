package eu.europeana.cloud.client.uis.rest.web;

import static eu.europeana.cloud.common.utils.UrlUtils.removeLastSlash;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * URL provider for UIS client
 * <p>
 * Always returns the same URL.
 *
 * @author Yorgos.Mamakis@ kb.nl
 * @since Dec 17, 2013
 */
public class StaticUrlProvider implements UrlProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(StaticUrlProvider.class);
  private String baseUrl;


  /**
   * Creates a new instance of this class.
   *
   * @param serviceUrl
   */
  public StaticUrlProvider(final String serviceUrl) {
    LOGGER.info("StaticUrlProvider: starting UrlProvider with serviceUrl='{}'", serviceUrl);
    baseUrl = removeLastSlash(serviceUrl);
    LOGGER.info("StaticUrlProvider: urlProvider started successfully.");
  }

  /**
   * Return the base url for the unique identifier service
   *
   * @return The base url for the service
   */
  public String getBaseUrl() {
    return baseUrl;
  }
}
