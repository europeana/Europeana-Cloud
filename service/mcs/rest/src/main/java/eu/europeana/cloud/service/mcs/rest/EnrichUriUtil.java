package eu.europeana.cloud.service.mcs.rest;

import static eu.europeana.cloud.common.web.ParamConstants.*;

import java.net.URI;

import javax.ws.rs.core.UriInfo;

import com.google.common.collect.ImmutableMap;

import eu.europeana.cloud.common.model.*;

/**
 * Utility class that inserts absolute uris into classes that will be used as REST responses.
 */
final class EnrichUriUtil {

    static void enrich(UriInfo uriInfo, Record record) {
        for (Representation rep : record.getRepresentations()) {
            enrich(uriInfo, rep);
        }
    }


    static void enrich(UriInfo uriInfo, Representation representation) {
        URI allVersionsUri = uriInfo
                .getBaseUriBuilder()
                .path(RepresentationVersionsResource.class)
                .buildFromMap(
                        ImmutableMap.of(P_GID, representation.getRecordId(), P_SCHEMA, representation.getSchema()));
        representation.setAllVersionsUri(uriInfo.resolve(allVersionsUri));

        if (representation.getVersion() != null) {
            URI latestVersionUri = uriInfo
                    .getBaseUriBuilder()
                    .path(RepresentationVersionResource.class)
                    .buildFromMap(
                            ImmutableMap.of(P_GID, representation.getRecordId(), P_SCHEMA, representation.getSchema(),
                                    P_VER, representation.getVersion()));
            representation.setUri(uriInfo.resolve(latestVersionUri));
        }
        if (representation.getFiles() != null) {
            for (File f : representation.getFiles()) {
                enrich(uriInfo, representation, f);
            }
        }
    }


    static void enrich(UriInfo uriInfo, Representation rep, File file) {
        enrich(uriInfo, rep.getRecordId(), rep.getSchema(), rep.getVersion(), file);
    }


    static void enrich(UriInfo uriInfo, String recordId, String schema, String version, File file) {
        URI fileUri = uriInfo
                .getBaseUriBuilder()
                .path(FileResource.class)
                .buildFromMap(
                        ImmutableMap.of(P_GID, recordId, P_SCHEMA, schema, P_VER, version, P_FILE, file.getFileName()));
        file.setContentUri(uriInfo.resolve(fileUri));
    }


    


    static void enrich(UriInfo uriInfo, DataSet dataSet) {
        URI datasetUri = uriInfo.getBaseUriBuilder().path(DataSetResource.class)
                .buildFromMap(ImmutableMap.of(P_PROVIDER, dataSet.getProviderId(), P_DATASET, dataSet.getId()));
        dataSet.setUri(uriInfo.resolve(datasetUri));
    }


    private EnrichUriUtil() {
    }
}
