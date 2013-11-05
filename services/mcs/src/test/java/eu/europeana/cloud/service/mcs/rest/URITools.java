package eu.europeana.cloud.service.mcs.rest;

import java.net.URI;
import java.util.Map;

import javax.ws.rs.core.UriBuilder;

import com.google.common.collect.ImmutableMap;

class URITools {

	static final String representationPath = "records/{ID}/representations/{REPRESENTATION}";
	static final String allVersionsPath = representationPath + "/versions";
	static final String VersionPath = representationPath
			+ "/versions/{VERSION}";
	static final String contentPath = representationPath
			+ "/versions/{VERSION}/files/{FILE}";

	static URI getAllVersionsUri(URI baseUri, String globalId,
			String representationName) {
		return UriBuilder.fromUri(baseUri.toString() + allVersionsPath)
				.buildFromMap(getTemplateValues(globalId, representationName));
	}

	static URI getVersionUri(URI baseUri, String globalId,
			String representationName, String version) {
		return UriBuilder
				.fromUri(baseUri.toString() + VersionPath)
				.buildFromMap(
						getTemplateValues(globalId, representationName, version));
	}

	static URI getRepresentationPath(String globalId, String representationName) {
		return UriBuilder.fromUri(representationPath).buildFromMap(
				getTemplateValues(globalId, representationName));
	}

	static URI getContetntUri(URI baseUri, String globalId,
			String representationName, String version, String fileName) {
		return UriBuilder.fromUri(baseUri.toString() + contentPath)
				.buildFromMap(
						getTemplateValues(globalId, representationName,
								version, fileName));
	}

	static URI getRepresentationsPath(String globalId) {
		return UriBuilder.fromResource(RepresentationsResource.class)
				.buildFromMap(getTemplateValues(globalId));
	}

	private static Map<String, ?> getTemplateValues(String globalId) {
		return ImmutableMap.<String, Object> of(ParamConstants.P_GID, globalId);
	}

	private static Map<String, Object> getTemplateValues(String globalId,
			String representationName) {
		return ImmutableMap.<String, Object> of(ParamConstants.P_GID, globalId,
				ParamConstants.P_REP, representationName);
	}

	private static Map<String, Object> getTemplateValues(String globalId,
			String representationName, String version) {
		return ImmutableMap.<String, Object> of(ParamConstants.P_GID, globalId,
				ParamConstants.P_REP, representationName, ParamConstants.P_VER,
				version);
	}

	private static Map<String, Object> getTemplateValues(String globalId,
			String representationName, String version, String fileName) {
		return ImmutableMap.<String, Object> of(ParamConstants.P_GID, globalId,
				ParamConstants.P_REP, representationName, ParamConstants.P_VER,
				version, ParamConstants.P_FILE, fileName);
	}

}
