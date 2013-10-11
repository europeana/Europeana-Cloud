package eu.europeana.cloud.contentserviceapi.rest;

import java.net.URI;

import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import eu.europeana.cloud.contentserviceapi.model.Record;
import static eu.europeana.cloud.contentserviceapi.rest.PathConstants.*;

import eu.europeana.cloud.contentserviceapi.model.Representation;

/**
 * EnrichUriUtil
 */
final class EnrichUriUtil {

    static void enrich(UriInfo uriInfo, Record record) {
        for (Representation rep : record.getRepresentations()) {
            enrich(uriInfo, rep);
        }
    }


    static void enrich(UriInfo uriInfo, Representation representation) {
        UriBuilder versionsUriBuilder = uriInfo.getBaseUriBuilder().segment(
                RECORDS, representation.getRecordId(),
                REPRESENTATIONS, representation.getSchema(),
                VERSIONS);
        representation.setAllVersionsUri(versionsUriBuilder.build());
        if (representation.getVersion() != null) {
            URI latestVersionUri = versionsUriBuilder.path(representation.getVersion()).build();
            representation.setSelfUri(latestVersionUri);
        }
    }
}
