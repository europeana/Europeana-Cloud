package eu.europeana.cloud.service.mcs.rest;

import com.google.common.collect.ImmutableMap;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.P_DATASET;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.P_FILE;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.P_GID;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.P_PROVIDER;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.P_SCHEMA;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.P_VER;
import java.net.URI;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

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
        URI allVersionsUri = UriBuilder.fromResource(RepresentationVersionsResource.class).buildFromMap(ImmutableMap.of(
                P_GID, representation.getRecordId(),
                P_SCHEMA, representation.getSchema()));
        representation.setAllVersionsUri(uriInfo.resolve(allVersionsUri));

        if (representation.getVersion() != null) {
            URI latestVersionUri = UriBuilder.fromResource(RepresentationVersionResource.class).buildFromMap(ImmutableMap.of(
                    P_GID, representation.getRecordId(),
                    P_SCHEMA, representation.getSchema(),
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
        URI fileUri = UriBuilder.fromResource(FileResource.class).buildFromMap(ImmutableMap.of(
                P_GID, recordId,
                P_SCHEMA, schema,
                P_VER, version,
                P_FILE, file.getFileName()));
        file.setContentUri(uriInfo.resolve(fileUri));
    }


    static void enrich(UriInfo uriInfo, DataProvider provider) {
        URI providerUri = UriBuilder.fromResource(DataProviderResource.class).buildFromMap(ImmutableMap.of(
                P_PROVIDER, provider.getId()));
        provider.setUri(uriInfo.resolve(providerUri));
    }


    static void enrich(UriInfo uriInfo, DataSet dataSet) {
        URI datasetUri = UriBuilder.fromResource(DataSetResource.class).buildFromMap(ImmutableMap.of(
                P_PROVIDER, dataSet.getProviderId(),
                P_DATASET, dataSet.getId()));
        dataSet.setUri(uriInfo.resolve(datasetUri));
    }


    private EnrichUriUtil() {
    }
}
