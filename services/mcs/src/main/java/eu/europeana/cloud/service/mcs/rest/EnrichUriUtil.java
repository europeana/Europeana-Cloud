package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataSet;
import java.net.URI;

import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import static eu.europeana.cloud.service.mcs.rest.PathConstants.*;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;

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
    
    static void enrich(UriInfo uriInfo, DataProvider provider) {
        UriBuilder providerUriBuilder = uriInfo.getBaseUriBuilder().segment(PROVIDERS, provider.getId());
        provider.setUri(providerUriBuilder.build());
    }

    static void enrich(UriInfo uriInfo, DataSet dataSet) {
        UriBuilder providerUriBuilder = uriInfo.getBaseUriBuilder().segment(PROVIDERS, dataSet.getProviderId(), DATASETS, dataSet.getId());
        dataSet.setUri(providerUriBuilder.build());
    }
}
