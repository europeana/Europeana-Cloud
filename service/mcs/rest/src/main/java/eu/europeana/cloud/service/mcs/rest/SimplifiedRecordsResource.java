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
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

import static eu.europeana.cloud.common.web.ParamConstants.LOCAL_ID;
import static eu.europeana.cloud.common.web.ParamConstants.PROVIDER_ID;

/**
 * Gives (read) access to record stored in ecloud in simplified (friendly) way.
 */
@RestController
@RequestMapping("/data-providers/{"+PROVIDER_ID+"}/records/{"+LOCAL_ID+":.+}")
@Scope("request")
public class SimplifiedRecordsResource {

    @Autowired
    private RecordService recordService;

    @Autowired
    private UISClientHandler uisHandler;

    /**
     * Returns record with all representations
     *
     * @param httpServletRequest
     * @param providerId providerId
     * @param localId    localId
     * @return record with all representations
     * @throws CloudException
     * @throws RecordNotExistsException
     * @summary Get record using simplified url
     */
    @GetMapping(produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
    public @ResponseBody Record getRecord(
            HttpServletRequest httpServletRequest,
            @PathVariable(PROVIDER_ID) String providerId,
            @PathVariable(LOCAL_ID) String localId) throws RecordNotExistsException, ProviderNotExistsException {

            final String cloudId = findCloudIdFor(providerId, localId);

            Record record = recordService.getRecord(cloudId);
            prepare(httpServletRequest, record);
            return record;
    }

    private String findCloudIdFor(String providerId, String localId) throws ProviderNotExistsException, RecordNotExistsException {
        CloudId foundCloudId =  uisHandler.getCloudIdFromProviderAndLocalId(providerId, localId);
        return foundCloudId.getId();
    }
    
    private void prepare(HttpServletRequest httpServletRequest, Record record) {
        EnrichUriUtil.enrich(httpServletRequest, record);
        for (Representation representation : record.getRepresentations()) {
            representation.setCloudId(null);
        }
    }
}
