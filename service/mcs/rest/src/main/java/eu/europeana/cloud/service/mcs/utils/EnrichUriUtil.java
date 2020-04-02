package eu.europeana.cloud.service.mcs.utils;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.service.mcs.rest.DataSetResource;
import eu.europeana.cloud.service.mcs.rest.FileResource;
import eu.europeana.cloud.service.mcs.rest.RepresentationVersionResource;
import eu.europeana.cloud.service.mcs.rest.RepresentationVersionsResource;

import javax.ws.rs.core.UriInfo;
import java.net.URI;

import static eu.europeana.cloud.common.web.ParamConstants.*;

/**
 * Utility class that inserts absolute uris into classes that will be used as REST responses.
 */
public final class EnrichUriUtil {
    private EnrichUriUtil() {
    }

    public static void enrich(UriInfo uriInfo, Record record) {
        for (Representation rep : record.getRepresentations()) {
            enrich(uriInfo, rep);
        }
    }

    public static void enrich(UriInfo uriInfo, Representation representation) {
        URI allVersionsUri = uriInfo
                .getBaseUriBuilder()
                .path(RepresentationVersionsResource.class)
                .buildFromMap(
                    ImmutableMap.of(P_CLOUDID, representation.getCloudId(), P_REPRESENTATIONNAME,
                        representation.getRepresentationName()));
        representation.setAllVersionsUri(uriInfo.resolve(allVersionsUri));

        if (representation.getVersion() != null) {
            URI latestVersionUri = uriInfo
                    .getBaseUriBuilder()
                    .path(RepresentationVersionResource.class)
                    .buildFromMap(
                        ImmutableMap.of(P_CLOUDID, representation.getCloudId(), P_REPRESENTATIONNAME,
                            representation.getRepresentationName(), P_VER, representation.getVersion()));
            representation.setUri(uriInfo.resolve(latestVersionUri));
        }
        if (representation.getFiles() != null) {
            for (File f : representation.getFiles()) {
                enrich(uriInfo, representation, f);
            }
        }
    }

    public static void enrich(UriInfo uriInfo, RepresentationRevisionResponse representationRevision) {
        if (representationRevision.getFiles() != null) {
            for (File f : representationRevision.getFiles()) {
                enrich(uriInfo, representationRevision.getCloudId(), representationRevision.getRepresentationName(), representationRevision.getVersion(), f);
            }
        }
    }

    public static void enrich(UriInfo uriInfo, Representation rep, File file) {
        enrich(uriInfo, rep.getCloudId(), rep.getRepresentationName(), rep.getVersion(), file);
    }


    public static void enrich(UriInfo uriInfo, String recordId, String schema, String version, File file) {
        URI fileUri = uriInfo
                .getBaseUriBuilder()
                .path(FileResource.class)
                .buildFromMap(
                    ImmutableMap.of(P_CLOUDID, recordId, P_REPRESENTATIONNAME, schema, P_VER, version, P_FILENAME,
                        file.getFileName()));
        file.setContentUri(uriInfo.resolve(fileUri));
    }


    public static void enrich(UriInfo uriInfo, DataSet dataSet) {
        URI datasetUri = uriInfo.getBaseUriBuilder().path(DataSetResource.class)
                .buildFromMap(ImmutableMap.of(P_PROVIDER, dataSet.getProviderId(), P_DATASET, dataSet.getId()));
        dataSet.setUri(uriInfo.resolve(datasetUri));
    }
}
