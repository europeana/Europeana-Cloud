package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import static eu.europeana.cloud.common.web.ParamConstants.P_LOCALID;
import static eu.europeana.cloud.common.web.ParamConstants.P_PROVIDER;

/**
 * Gives (read) access to record stored in ecloud in simplified (friendly) way.
 */
@RestController
@RequestMapping("/data-providers/{" + P_PROVIDER + "}/records/{" + P_LOCALID + ":.+}")
@Scope("request")
public class SimplifiedRecordsResource {

    @Autowired
    private RecordService recordService;

    @Autowired
    private UISClientHandler uisHandler;

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
    @GetMapping(produces = {MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public Record getRecord(@Context UriInfo uriInfo,
                            @PathParam(P_PROVIDER) String providerId,
                            @PathParam(P_LOCALID) String localId) throws CloudException, RecordNotExistsException, ProviderNotExistsException {

            final String cloudId = findCloudIdFor(providerId, localId);

            Record record = recordService.getRecord(cloudId);
            prepare(uriInfo, record);
            return record;
    }

    private String findCloudIdFor(String providerId, String localId) throws ProviderNotExistsException, RecordNotExistsException {
        CloudId foundCloudId =  uisHandler.getCloudIdFromProviderAndLocalId(providerId, localId);
        return foundCloudId.getId();
    }
    
    private void prepare(@Context UriInfo uriInfo, Record record) {
        EnrichUriUtil.enrich(uriInfo, record);
        for (Representation representation : record.getRepresentations()) {
            representation.setCloudId(null);
        }
    }
}
