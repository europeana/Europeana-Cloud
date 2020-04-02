package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.UISClientHandler;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PostAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import static eu.europeana.cloud.common.web.ParamConstants.*;

/**
 * Gives access to latest persistent representation using 'friendly' URL
 */
@RestController
@RequestMapping("/data-providers/{" + P_PROVIDER + "}/records/{" + P_LOCALID + ":.+}/representations/{" + P_REPRESENTATIONNAME + "}")
@Scope("request")
public class SimplifiedRepresentationResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimplifiedRepresentationResource.class);

    @Autowired
    private UISClientHandler uisClientHandler;

    @Autowired
    private RecordService recordService;

    /**
     * Returns the latest persistent version of a given representation.
     *
     * @param uriInfo
     * @param providerId
     * @param localId
     * @param representationName
     * @return
     * @throws CloudException
     * @throws RepresentationNotExistsException
     * @summary Get representation using simplified url
     */
    @GetMapping
    @PostAuthorize("hasPermission"
            + "( "
            + " (returnObject.cloudId).concat('/').concat(#representationName).concat('/').concat(returnObject.version) ,"
            + " 'eu.europeana.cloud.common.model.Representation', read" + ")")
    public Representation getRepresentation(@Context UriInfo uriInfo,
                                            @PathParam(P_PROVIDER) String providerId,
                                            @PathParam(P_LOCALID) String localId,
                                            @PathParam(P_REPRESENTATIONNAME) String representationName) throws CloudException, RepresentationNotExistsException, ProviderNotExistsException, RecordNotExistsException {

        LOGGER.info("Reading representation '{}' using 'friendly' approach for providerId: {} and localId: {}", representationName, providerId, localId);
        final String cloudId = findCloudIdFor(providerId, localId);

        Representation representation = recordService.getRepresentation(cloudId, representationName);
        EnrichUriUtil.enrich(uriInfo, representation);

        return representation;
    }

    private String findCloudIdFor(String providerID, String localId) throws CloudException, ProviderNotExistsException, RecordNotExistsException {
        CloudId foundCloudId = uisClientHandler.getCloudIdFromProviderAndLocalId(providerID, localId);
        return foundCloudId.getId();
    }
}
