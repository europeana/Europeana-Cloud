package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.LocalRecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.uis.exception.RecordDoesNotExistException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import static eu.europeana.cloud.common.web.ParamConstants.P_LOCALID;
import static eu.europeana.cloud.common.web.ParamConstants.P_PROVIDER;

/**
 * Gives (read) access to record stored in ecloud in simplified (friendly) way.
 */
@Path("/data-providers/{" + P_PROVIDER + "}/records/{" + P_LOCALID + ":.+}")
@Component
@Scope("request")
public class SimplifiedRecordsResource {

    @Autowired
    private UISClient uisClient;

    @Autowired
    private RecordService recordService;

    /**
     * Returns record with all representations
     *
     * @param uriInfo
     * @param providerId providerId
     * @param localId    localId
     * @return record with all representations
     * @throws CloudException
     * @throws RecordNotExistsException
     * @summary Get record using simplified url
     */
    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Record getRecord(@Context UriInfo uriInfo,
                            @PathParam(P_PROVIDER) String providerId,
                            @PathParam(P_LOCALID) String localId) throws CloudException, RecordNotExistsException, LocalRecordNotExistsException {

        try {
            final String cloudId = findCloudIdFor(providerId, localId);

            Record record = recordService.getRecord(cloudId);
            prepare(uriInfo, record);
            return record;
        }
        catch (CloudException e) {
            if (e.getCause() instanceof RecordDoesNotExistException) {
                throw new LocalRecordNotExistsException(providerId, localId);
            }
            else
                throw e;
        }
    }

    private String findCloudIdFor(String providerId, String localId) throws CloudException {
        CloudId foundCloudId = uisClient.getCloudId(providerId, localId);
        return foundCloudId.getId();
    }
    
    private void prepare(@Context UriInfo uriInfo, Record record) {
        EnrichUriUtil.enrich(uriInfo, record);
        for (Representation representation : record.getRepresentations()) {
            representation.setCloudId(null);
        }
    }
}
