package eu.europeana.cloud.service.mcs.rest;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.web.ParamConstants;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Map;

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


    static URI getVersionPath(String globalId, String schema, String version) {
        return UriBuilder.fromResource(RepresentationVersionResource.class).buildFromMap(
            getVersionMap(globalId, schema, version));
    }


    static URI getRepresentationPath(String globalId, String schema) {
        return UriBuilder.fromResource(RepresentationResource.class).buildFromMap(
            getRepresentationMap(globalId, schema));
    }


    static URI getRepresentationRevisionsPath(String globalId, String schema, String revisionId) {
        return UriBuilder.fromResource(RepresentationRevisionsResource.class).buildFromMap(
                getRepresentationRevisionsMap(globalId, schema, revisionId));
    }

    static URI getContentUri(URI baseUri, String globalId, String schema, String version, String fileName) {
        UriBuilder uriFromResource = UriBuilder.fromResource(FileResource.class);
        setBaseUri(uriFromResource, baseUri);
        return uriFromResource.buildFromMap(getFileMap(globalId, schema, version, fileName));
    }


    static URI getRepresentationsPath(String globalId) {
        return UriBuilder.fromResource(RepresentationsResource.class).buildFromMap(getGlobalIdMap(globalId));
    }


    static Object getListVersionsPath(String globalId, String schema) {
        return UriBuilder.fromResource(RepresentationVersionsResource.class).buildFromMap(
            getRepresentationMap(globalId, schema));
    }


    static URI getPath(Class<RepresentationVersionResource> resourceClass, String method, String globalId,
            String schema, String version) {
        return UriBuilder.fromResource(resourceClass).path(resourceClass, method)
                .buildFromMap(getVersionMap(globalId, schema, version));
    }


    private static Map<String, String> getGlobalIdMap(String globalId) {
        return ImmutableMap.<String, String> of(ParamConstants.P_CLOUDID, globalId);
    }


    private static Map<String, String> getRepresentationMap(String globalId, String schema) {
        return ImmutableMap.<String, String> of(ParamConstants.P_CLOUDID, globalId,
            ParamConstants.P_REPRESENTATIONNAME, schema);
    }


    private static Map<String, String> getRepresentationRevisionsMap(String globalId, String schema, String revisionId) {
        return ImmutableMap.<String, String> of(ParamConstants.P_CLOUDID, globalId,
                ParamConstants.P_REPRESENTATIONNAME, schema, ParamConstants.REVISION_NAME, revisionId);
    }


    private static Map<String, String> getVersionMap(String globalId, String schema, String version) {
        return ImmutableMap.<String, String> of(ParamConstants.P_CLOUDID, globalId,
            ParamConstants.P_REPRESENTATIONNAME, schema, ParamConstants.P_VER, version);
    }


    private static Map<String, String> getFileMap(String globalId, String schema, String version, String fileName) {
        return ImmutableMap.<String, String> of(ParamConstants.P_CLOUDID, globalId,
            ParamConstants.P_REPRESENTATIONNAME, schema, ParamConstants.P_VER, version, ParamConstants.P_FILENAME,
            fileName);
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
