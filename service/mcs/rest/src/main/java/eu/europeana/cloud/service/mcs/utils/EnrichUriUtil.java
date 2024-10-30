package eu.europeana.cloud.service.mcs.utils;

import static eu.europeana.cloud.common.web.ParamConstants.CLOUD_ID;
import static eu.europeana.cloud.common.web.ParamConstants.DATA_SET_ID;
import static eu.europeana.cloud.common.web.ParamConstants.FILE_NAME;
import static eu.europeana.cloud.common.web.ParamConstants.PROVIDER_ID;
import static eu.europeana.cloud.common.web.ParamConstants.REPRESENTATION_NAME;
import static eu.europeana.cloud.common.web.ParamConstants.VERSION;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.CLIENT_FILE_RESOURCE;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.DATA_SET_RESOURCE;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATION_VERSION;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATION_VERSIONS_RESOURCE;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;

import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpRequest;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Utility class that inserts absolute uris into classes that will be used as REST responses.
 */
public final class EnrichUriUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(EnrichUriUtil.class);

  private static final MappingPlaceholdersResolver PROPERTY_PLACEHOLDER_HELPER
      = new MappingPlaceholdersResolver();

  private EnrichUriUtil() {
  }

  public static void enrich(HttpServletRequest httpServletRequest, Record record) {
    for (Representation rep : record.getRepresentations()) {
      enrich(httpServletRequest, rep);
    }
  }

  public static void enrich(HttpServletRequest httpServletRequest, Representation representation) {
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

  public static void enrich(HttpServletRequest httpServletRequest, Representation rep, File file) {
    enrich(httpServletRequest, rep.getCloudId(), rep.getRepresentationName(), rep.getVersion(), file);
  }

  public static void enrich(
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

  public static void enrich(HttpServletRequest httpServletRequest, DataSet dataSet) {
    Properties properties = new Properties();

    properties.setProperty(PROVIDER_ID, dataSet.getProviderId());
    properties.setProperty(DATA_SET_ID, dataSet.getId());

    dataSet.setUri(createURI(httpServletRequest, DATA_SET_RESOURCE, properties));
  }

  public static void enrich(HttpServletRequest httpServletRequest, RepresentationRevisionResponse response) {
    Properties properties = new Properties();
    properties.setProperty(CLOUD_ID, response.getCloudId());
    properties.setProperty(REPRESENTATION_NAME, response.getRepresentationName());
    properties.setProperty(VERSION, response.getVersion());
    response.setRepresentationVersionUri(createURI(httpServletRequest, REPRESENTATION_VERSION, properties));
    if (response.getFiles() != null) {
      for (File file : response.getFiles()) {
        EnrichUriUtil.enrich(httpServletRequest, response.getCloudId(), response.getRepresentationName(),
            response.getVersion(), file);
      }
    }
  }

  private static URI createURI(HttpServletRequest httpServletRequest, String path, Properties properties) {
    URI result = null;

    StringBuilder uriSpec = new StringBuilder();
    uriSpec.append(httpServletRequest.getContextPath());
    uriSpec.append(PROPERTY_PLACEHOLDER_HELPER.replacePlaceholders(path, properties));
    try {
      HttpRequest httpRequest = new ServletServerHttpRequest(httpServletRequest);
      URL url = new URL(
          UriComponentsBuilder.fromHttpRequest(httpRequest).build().getScheme(),
          httpServletRequest.getServerName(),
          (httpServletRequest.getServerPort() == 80 || httpServletRequest.getServerPort() == 443) ?
              -1 : httpServletRequest.getServerPort(),  //Do not setup 80 (http) or 443 (https) ports - pointed by scheme
          uriSpec.toString());

      result = url.toURI();
    } catch (MalformedURLException | URISyntaxException ure) {
      LOGGER.warn("Invalid URL/URI", ure);
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
