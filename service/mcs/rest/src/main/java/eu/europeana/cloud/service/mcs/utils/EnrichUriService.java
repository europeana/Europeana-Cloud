package eu.europeana.cloud.service.mcs.utils;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpRequest;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import static eu.europeana.cloud.common.web.ParamConstants.*;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.*;

/**
 * Utility class that inserts absolute uris into classes that will be used as REST responses.
 */
public final class EnrichUriService {

  private final Logger LOGGER = LoggerFactory.getLogger(EnrichUriService.class);

  private final MappingPlaceholdersResolver PROPERTY_PLACEHOLDER_HELPER
      = new MappingPlaceholdersResolver();

  private final URI MCS_EXTERNAL_URI;

  public EnrichUriService(String mcsExternalUri) {
    try {
      this.MCS_EXTERNAL_URI = new URI(mcsExternalUri);
    } catch (URISyntaxException e) {
      throw new RuntimeException("Invalid MCS base URI: " + mcsExternalUri, e);
    }
  }

  public void enrich(HttpServletRequest httpServletRequest, Record record) {
    for (Representation rep : record.getRepresentations()) {
      enrich(httpServletRequest, rep);
    }
  }

  public void enrich(HttpServletRequest httpServletRequest, Representation representation) {
    Properties properties = new Properties();
    properties.setProperty(CLOUD_ID, representation.getCloudId());
    properties.setProperty(REPRESENTATION_NAME, representation.getRepresentationName());

    representation.setAllVersionsUri(createURI(httpServletRequest, REPRESENTATION_VERSIONS_RESOURCE, properties));

    if (representation.getVersion() != null) {
      properties.setProperty(VERSION, representation.getVersion());
      representation.setUri(createURI(httpServletRequest, REPRESENTATION_VERSION, properties));
    }

    if (representation.getFiles() != null) {
      for (File f : representation.getFiles()) {
        enrich(httpServletRequest, representation, f);
      }
    }

  }

  public void enrich(HttpServletRequest httpServletRequest, Representation rep, File file) {
    enrich(httpServletRequest, rep.getCloudId(), rep.getRepresentationName(), rep.getVersion(), file);
  }

  public void enrich(
      HttpServletRequest httpServletRequest,
      String cloudId,
      String representationName,
      String version,
      File file) {
    Properties properties = new Properties();
    properties.setProperty(CLOUD_ID, cloudId);
    properties.setProperty(REPRESENTATION_NAME, representationName);
    properties.setProperty(VERSION, version);
    properties.setProperty(FILE_NAME, file.getFileName());

    file.setContentUri(createURI(httpServletRequest, CLIENT_FILE_RESOURCE, properties));
  }

  public void enrich(HttpServletRequest httpServletRequest, DataSet dataSet) {
    Properties properties = new Properties();

    properties.setProperty(PROVIDER_ID, dataSet.getProviderId());
    properties.setProperty(DATA_SET_ID, dataSet.getId());

    dataSet.setUri(createURI(httpServletRequest, DATA_SET_RESOURCE, properties));
  }

  /**
   * Enriches the response with valid URIs (the URI is dependent on servlet request headers).
   *
   * @param httpServletRequest - request object
   * @param response - enriched response
   */
  public void enrich(HttpServletRequest httpServletRequest, RepresentationRevisionResponse response) {
    Properties properties = new Properties();
    properties.setProperty(CLOUD_ID, response.getCloudId());
    properties.setProperty(REPRESENTATION_NAME, response.getRepresentationName());
    properties.setProperty(VERSION, response.getVersion());
    response.setRepresentationVersionUri(createURI(httpServletRequest, REPRESENTATION_VERSION, properties));
    if (response.getFiles() != null) {
      for (File file : response.getFiles()) {
        enrich(httpServletRequest, response.getCloudId(), response.getRepresentationName(),
            response.getVersion(), file);
      }
    }
  }

  private URI createURI(HttpServletRequest httpServletRequest, String path, Properties properties) {
    URI result = null;

    StringBuilder uriSpec = new StringBuilder();
    uriSpec.append(httpServletRequest.getContextPath());
    uriSpec.append(PROPERTY_PLACEHOLDER_HELPER.replacePlaceholders(path, properties));

    try {
      HttpRequest httpRequest = new ServletServerHttpRequest(httpServletRequest);

      UriComponentsBuilder builder = UriComponentsBuilder.fromUri(httpRequest.getURI());

      if (this.MCS_EXTERNAL_URI != null) {
        builder.scheme(this.MCS_EXTERNAL_URI.getScheme())
                .host(this.MCS_EXTERNAL_URI.getHost())
                .port(this.MCS_EXTERNAL_URI.getPort())
                .userInfo(this.MCS_EXTERNAL_URI.getUserInfo());
      }

      result = builder
              .replacePath(uriSpec.toString())
              .build()
              .toUri();

    } catch (IllegalArgumentException e) {
      LOGGER.warn("Invalid URL/URI", e);
    }

    return result;
  }

  public static class MappingPlaceholdersResolver {

    private static final char OPEN = '{';
    private static final char CLOSE = '}';
    private static final char REG_EXP_SEPRATOR = ':';


    public String replacePlaceholders(String path, Properties properties) {
      StringBuilder result = new StringBuilder();

      int currentIndex = 0;
      int prevIndex = 0;

      while (currentIndex != -1) {
        currentIndex = path.indexOf(OPEN, prevIndex);
        if (currentIndex != -1) {
          result.append(path.substring(prevIndex, currentIndex));

          int closeIndex = path.indexOf(CLOSE, currentIndex);
          if (closeIndex == -1) {
            result.append(path.substring(prevIndex));
          } else {
            prevIndex = processPlaceholder(path.substring(currentIndex, closeIndex + 1), properties, closeIndex, result);
          }
        } else {
          result.append(path.substring(prevIndex));
        }
      }
      return result.toString();
    }

    private int processPlaceholder(String placeHolder, Properties properties, int closeIndex, StringBuilder result) {
      int regExpSepLocalIndex = placeHolder.indexOf(REG_EXP_SEPRATOR);

      String placeholderName;
      String placeholderRegExp = null;

      if (regExpSepLocalIndex == -1) {
        placeholderName = placeHolder.substring(1, placeHolder.length() - 1).trim();
      } else {
        placeholderName = placeHolder.substring(1, regExpSepLocalIndex).trim();
        placeholderRegExp = placeHolder.substring(regExpSepLocalIndex + 1, placeHolder.length() - 1).trim();
      }

      if (!properties.containsKey(placeholderName)) {
        result.append(placeHolder);
      } else {
        String value = properties.getProperty(placeholderName);

        if (placeholderRegExp == null || value.matches(placeholderRegExp)) {
          result.append(value);
        } else {
          result.append(placeHolder);
        }
      }

      return closeIndex + 1;
    }

  }
}
