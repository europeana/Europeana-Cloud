package eu.europeana.cloud.service.mcs.rest;

import java.net.URI;
import java.util.Map;

import javax.ws.rs.core.UriBuilder;

import com.google.common.collect.ImmutableMap;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;

class URITools {

	static URI getAllVersionsUri(URI baseUri, String globalId,
			String representationName) {
		UriBuilder uriFromResource = UriBuilder
				.fromResource(RepresentationVersionsResource.class);
		setBaseUri(uriFromResource, baseUri);
		return uriFromResource.buildFromMap(getRepresentationMap(globalId,
				representationName));
	}

	private static void setBaseUri(UriBuilder uriFromResource, URI baseUri) {
		uriFromResource.scheme(baseUri.getScheme()).host(baseUri.getHost())
				.port(baseUri.getPort());
	}

	static URI getVersionUri(URI baseUri, String globalId,
			String representationName, String version) {
		UriBuilder uriFromResource = UriBuilder
				.fromResource(RepresentationVersionResource.class);
		setBaseUri(uriFromResource, baseUri);
		return uriFromResource.buildFromMap(getVersionMap(globalId,
				representationName, version));
	}

	static URI getVersionPath(String globalId, String representationName,
			String version) {
		return UriBuilder.fromResource(RepresentationVersionResource.class)
				.buildFromMap(
						getVersionMap(globalId, representationName, version));
	}

	static URI getRepresentationPath(String globalId, String representationName) {
		return UriBuilder.fromResource(RepresentationResource.class)
				.buildFromMap(
						getRepresentationMap(globalId, representationName));
	}

	static URI getContentUri(URI baseUri, String globalId,
			String representationName, String version, String fileName) {
		UriBuilder uriFromResource = UriBuilder
				.fromResource(FileResource.class);
		setBaseUri(uriFromResource, baseUri);
		return uriFromResource.buildFromMap(getFileMap(globalId,
				representationName, version, fileName));
	}

	static URI getRepresentationsPath(String globalId) {
		return UriBuilder.fromResource(RepresentationsResource.class)
				.buildFromMap(getGlobalIdMap(globalId));
	}

	static Object getListVersionsPath(String globalId, String representationName) {
		return UriBuilder.fromResource(RepresentationVersionsResource.class)
				.buildFromMap(
						getRepresentationMap(globalId, representationName));
	}

	static URI getPath(Class<RepresentationVersionResource> resourceClass,
			String method, String globalId, String representationName,
			String version) {
		return UriBuilder
				.fromResource(resourceClass)
				.path(resourceClass, method)
				.buildFromMap(
						getVersionMap(globalId, representationName, version));
	}

	private static Map<String, String> getGlobalIdMap(String globalId) {
		return ImmutableMap.<String, String> of(ParamConstants.P_GID, globalId);
	}

	private static Map<String, String> getRepresentationMap(String globalId,
			String representationName) {
		return ImmutableMap.<String, String> of(ParamConstants.P_GID, globalId,
				ParamConstants.P_REP, representationName);
	}

	private static Map<String, String> getVersionMap(String globalId,
			String representationName, String version) {
		return ImmutableMap.<String, String> of(ParamConstants.P_GID, globalId,
				ParamConstants.P_REP, representationName, ParamConstants.P_VER,
				version);
	}

	private static Map<String, String> getFileMap(String globalId,
			String representationName, String version, String fileName) {
		return ImmutableMap.<String, String> of(ParamConstants.P_GID, globalId,
				ParamConstants.P_REP, representationName, ParamConstants.P_VER,
				version, ParamConstants.P_FILE, fileName);
	}

	static void enrich(Representation representation, URI baseUri) {
		representation.setUri(URITools.getVersionUri(baseUri, representation.getRecordId(),
				representation.getSchema(), representation.getVersion()));
		representation.setAllVersionsUri(URITools.getAllVersionsUri(baseUri,
				representation.getRecordId(), representation.getSchema()));
		for (File file : representation.getFiles()) {
			file.setContentUri(URITools.getContentUri(baseUri, representation.getRecordId(),
					representation.getSchema(), representation.getVersion(), file.getFileName()));
		}
	}

}
