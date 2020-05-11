package eu.europeana.cloud.service.mcs.rest;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.web.ParamConstants;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Map;

import static eu.europeana.cloud.common.web.ParamConstants.*;

class URITools {

    static URI getAllVersionsUri(URI baseUri, String globalId, String schema) {
        UriBuilder uriFromResource = UriBuilder.fromResource(RepresentationVersionsResource.class);
        setBaseUri(uriFromResource, baseUri);
        return uriFromResource.buildFromMap(getRepresentationMap(globalId, schema));
    }


    private static void setBaseUri(UriBuilder uriFromResource, URI baseUri) {
        uriFromResource.scheme(baseUri.getScheme()).host(baseUri.getHost()).port(baseUri.getPort());
    }


    static URI getVersionUri(URI baseUri, String globalId, String schema, String version) {
        UriBuilder uriFromResource = UriBuilder.fromResource(RepresentationVersionResource.class);
        setBaseUri(uriFromResource, baseUri);
        return uriFromResource.buildFromMap(getVersionMap(globalId, schema, version));
    }


    static URI getVersionPath(String cloudId, String representationName, String version) {
        return UriBuilder.fromResource(RepresentationVersionResource.class).buildFromMap(
            getVersionMap(cloudId, representationName, version));
    }


    static URI getRepresentationPath(String cloudId, String representationName) {
        return UriBuilder.fromResource(RepresentationResource.class).buildFromMap(
            getRepresentationMap(cloudId, representationName));
    }


    static URI getRepresentationRevisionsPath(String cloudId, String representationName, String revisionId) {
        return UriBuilder.fromResource(RepresentationRevisionsResource.class).buildFromMap(
                getRepresentationRevisionsMap(cloudId, representationName, revisionId));
    }

    static URI getContentUri(URI baseUri, String cloudId, String representationName, String version, String fileName) {
        UriBuilder uriFromResource = UriBuilder.fromResource(FileResource.class);
        setBaseUri(uriFromResource, baseUri);
        return uriFromResource.buildFromMap(getFileMap(cloudId, representationName, version, fileName));
    }


    static URI getRepresentationsPath(String cloudId) {
        return UriBuilder.fromResource(RepresentationsResource.class).buildFromMap(getGlobalIdMap(cloudId));
    }


    static Object getListVersionsPath(String cloudId, String representationName) {
        return UriBuilder.fromResource(RepresentationVersionsResource.class).buildFromMap(
            getRepresentationMap(cloudId, representationName));
    }


    static URI getPath(Class<RepresentationVersionResource> resourceClass, String method, String cloudId,
            String representationName, String version) {
        return UriBuilder.fromResource(resourceClass).path(resourceClass, method)
                .buildFromMap(getVersionMap(cloudId, representationName, version));
    }


    private static Map<String, String> getGlobalIdMap(String cloudId) {
        return ImmutableMap.<String, String> of(CLOUD_ID, cloudId);
    }


    private static Map<String, String> getRepresentationMap(String cloudId, String representationName) {
        return ImmutableMap.<String, String> of(
                CLOUD_ID, cloudId,
                REPRESENTATION_NAME, representationName);
    }


    private static Map<String, String> getRepresentationRevisionsMap(String cloudId, String representationName, String revisionId) {
        return ImmutableMap.<String, String> of(
                CLOUD_ID, cloudId,
                REPRESENTATION_NAME, representationName,
                REVISION_NAME, revisionId);
    }


    private static Map<String, String> getVersionMap(String cloudId, String representationName, String version) {
        return ImmutableMap.<String, String> of(
                CLOUD_ID, cloudId,
                REPRESENTATION_NAME, representationName,
                VERSION, version);
    }


    private static Map<String, String> getFileMap(String cloudId, String representationName, String version, String fileName) {
        return ImmutableMap.<String, String> of(
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

}
