package eu.europeana.cloud.service.mcs.controller;

import static eu.europeana.cloud.common.web.ParamConstants.CLOUD_ID;
import static eu.europeana.cloud.common.web.ParamConstants.FILE_NAME;
import static eu.europeana.cloud.common.web.ParamConstants.REPRESENTATION_NAME;
import static eu.europeana.cloud.common.web.ParamConstants.REVISION_NAME;
import static eu.europeana.cloud.common.web.ParamConstants.VERSION;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATION_RAW_REVISIONS_RESOURCE;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATION_REVISIONS_RESOURCE;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATION_VERSION;
import static org.springframework.web.util.UriComponentsBuilder.fromUriString;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RestInterfaceConstants;
import java.net.URI;
import java.util.Map;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.util.UriComponentsBuilder;

class URITools {

  static URI getAllVersionsUri(URI baseUri, String globalId, String schema) {
    UriComponentsBuilder uriFromResource = fromResource(RepresentationVersionsResource.class);
    setBaseUri(uriFromResource, baseUri);
    return uriFromResource.build(getRepresentationMap(globalId, schema));
  }


  private static void setBaseUri(UriComponentsBuilder uriFromResource, URI baseUri) {
    uriFromResource.scheme(baseUri.getScheme()).host(baseUri.getHost()).port(baseUri.getPort());
  }


  static URI getVersionUri(URI baseUri, String globalId, String schema, String version) {
    UriComponentsBuilder uriFromResource = fromUriString(RestInterfaceConstants.REPRESENTATION_VERSION);
    setBaseUri(uriFromResource, baseUri);
    return uriFromResource.build(getVersionMap(globalId, schema, version));
  }


  static URI getVersionPath(String cloudId, String representationName, String version) {
    return fromUriString(REPRESENTATION_VERSION).build(
        getVersionMap(cloudId, representationName, version));
  }


  static URI getRepresentationPath(String cloudId, String representationName) {
    return fromResource(RepresentationResource.class).build(getRepresentationMap(cloudId, representationName));
  }

  static URI getRepresentationRevisionsPath(String cloudId, String representationName, String revisionId) {
    return fromUriString(REPRESENTATION_REVISIONS_RESOURCE).build(
        getRepresentationRevisionsMap(cloudId, representationName, revisionId));
  }

  static URI getRepresentationRawRevisionsPath(String cloudId, String representationName, String revisionId) {
    return fromUriString(REPRESENTATION_RAW_REVISIONS_RESOURCE).build(
        getRepresentationRevisionsMap(cloudId, representationName, revisionId));
  }

  static URI getContentUri(URI baseUri, String cloudId, String representationName, String version, String fileName) {
    UriComponentsBuilder uriFromResource = fromUriString(RestInterfaceConstants.CLIENT_FILE_RESOURCE);
    setBaseUri(uriFromResource, baseUri);
    return uriFromResource.build(getFileMap(cloudId, representationName, version, fileName));
  }


  static URI getRepresentationsPath(String cloudId) {
    return fromResource(RepresentationsResource.class).build(getGlobalIdMap(cloudId));
  }


  static Object getListVersionsPath(String cloudId, String representationName) {
    return fromResource(RepresentationVersionsResource.class).build(
        getRepresentationMap(cloudId, representationName));
  }


  private static Map<String, String> getGlobalIdMap(String cloudId) {
    return ImmutableMap.<String, String>of(CLOUD_ID, cloudId);
  }


  private static Map<String, String> getRepresentationMap(String cloudId, String representationName) {
    return ImmutableMap.<String, String>of(
        CLOUD_ID, cloudId,
        REPRESENTATION_NAME, representationName);
  }


  private static Map<String, String> getRepresentationRevisionsMap(String cloudId, String representationName, String revisionId) {
    return ImmutableMap.<String, String>of(
        CLOUD_ID, cloudId,
        REPRESENTATION_NAME, representationName,
        REVISION_NAME, revisionId);
  }


  private static Map<String, String> getVersionMap(String cloudId, String representationName, String version) {
    return ImmutableMap.<String, String>of(
        CLOUD_ID, cloudId,
        REPRESENTATION_NAME, representationName,
        VERSION, version);
  }


  private static Map<String, String> getFileMap(String cloudId, String representationName, String version, String fileName) {
    return ImmutableMap.<String, String>of(
        CLOUD_ID, cloudId,
        REPRESENTATION_NAME, representationName,
        VERSION, version,
        FILE_NAME, fileName);
  }


  static void enrich(Representation representation, URI baseUri) {
    representation.setUri(URITools.getVersionUri(baseUri, representation.getCloudId(),
        representation.getRepresentationName(), representation.getVersion()));
    representation.setAllVersionsUri(URITools.getAllVersionsUri(baseUri, representation.getCloudId(),
        representation.getRepresentationName()));
    for (File file : representation.getFiles()) {
      file.setContentUri(URITools.getContentUri(baseUri, representation.getCloudId(),
          representation.getRepresentationName(), representation.getVersion(), file.getFileName()));
    }
  }

  private static UriComponentsBuilder fromResource(Class<?> aClass) {
    String uriFromResourceAnnotation = aClass.getAnnotation(RequestMapping.class).value()[0];
    return fromUriString(uriFromResourceAnnotation);
  }

}
