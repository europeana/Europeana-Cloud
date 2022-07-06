package eu.europeana.cloud.service.uis.rest;

import com.qmino.miredot.annotations.ReturnType;
import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.uis.ACLServiceWrapper;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.RestInterfaceConstants;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.exception.*;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.model.*;
import org.springframework.web.bind.annotation.*;

/**
 * Resource for DataProvider.
 */
@RestController
public class DataProviderResource {

    private final UniqueIdentifierService uniqueIdentifierService;
    private final DataProviderService providerService;
    private final ACLServiceWrapper aclWrapper;

    public DataProviderResource(UniqueIdentifierService uniqueIdentifierService,
                                DataProviderService providerService,
                                ACLServiceWrapper aclWrapper) {
        this.uniqueIdentifierService = uniqueIdentifierService;
        this.providerService = providerService;
        this.aclWrapper = aclWrapper;
    }

    protected static final String LOCAL_ID_CLASS_NAME = "LocalId";


    /**
     * Retrieves details about selected data provider
     *
     * @param providerId
     *            <strong>REQUIRED</strong> identifier of the provider that will
     *            be retrieved
     *
     * @return Selected Data provider details
     *
     * @throws ProviderDoesNotExistException
     *             The supplied provider does not exist
     */
    @GetMapping(value = RestInterfaceConstants.DATA_PROVIDER, produces = { MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE })
    public DataProvider getProvider(@PathVariable String providerId) throws ProviderDoesNotExistException {
        return providerService.getProvider(providerId);
    }

    /**
     * Updates data provider information.
     *
     * <br/>
     * <br/>
     * <div style='border-left: solid 5px #999999; border-radius: 10px; padding:
     * 6px;'> <strong>Required permissions:</strong>
     * <ul>
     * <li>Authenticated user</li>
     * <li>Write permission for the selected data provider</li>
     * </ul>
     * </div>
     *
     * @param dataProviderProperties
     *            <strong>REQUIRED</strong> data provider properties.
     *
     * @param providerId
     *            <strong>REQUIRED</strong> identifier of data provider which
     *            will be updated.
     *
     * @statuscode 204 object has been updated.
     *
     * @throws ProviderDoesNotExistException
     *             The supplied provider does not exist
     */
    @PutMapping(value = RestInterfaceConstants.DATA_PROVIDER, consumes = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
    @PreAuthorize("hasPermission(#providerId, 'eu.europeana.cloud.common.model.DataProvider', write)")
    public ResponseEntity<Void> updateProvider(
            @RequestBody DataProviderProperties dataProviderProperties,
            @PathVariable String providerId) throws ProviderDoesNotExistException {

        providerService.updateProvider(providerId, dataProviderProperties);
        return ResponseEntity.noContent().build();
    }

    /**
     *
     * Create a mapping between a cloud identifier and a record identifier for a
     * provider
     *
     * <br/>
     * <br/>
     * <div style='border-left: solid 5px #999999; border-radius: 10px; padding:
     * 6px;'> <strong>Required permissions:</strong>
     * <ul>
     * <li>Authenticated user</li>
     * </ul>
     * </div>
     *
     * @param providerId
     *            <strong>REQUIRED</strong> identifier of data provider, owner
     *            of the record
     * @param cloudId
     *            <strong>REQUIRED</strong> cloud identifier for which new
     *            record identifier will be added
     * @param recordId
     *            record identifier which will be bound to selected cloud
     *            identifier. If not specified, random one will be generated
     *
     * @return The newly associated cloud identifier
     *
     * @throws DatabaseConnectionException
     *             database connection error
     * @throws CloudIdDoesNotExistException
     *             cloud identifier does not exist
     * @throws ProviderDoesNotExistException
     *             provider does not exist
     * @throws RecordDatasetEmptyException
     *             empty dataset
     * @throws CloudIdAlreadyExistException
     *             cloud identifier already exist
     *
     */
    @PostMapping(value = RestInterfaceConstants.CLOUD_ID_TO_RECORD_ID_MAPPING, produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE })
    @PreAuthorize("isAuthenticated()")
    @ReturnType("eu.europeana.cloud.common.model.CloudId")
    public ResponseEntity<CloudId> createIdMapping(
            @PathVariable String providerId,
            @PathVariable String cloudId,
            @RequestParam(required = false) String recordId)
            throws DatabaseConnectionException, CloudIdDoesNotExistException,
            ProviderDoesNotExistException, RecordDatasetEmptyException, CloudIdAlreadyExistException {

        CloudId result;
        if (recordId != null) {
            result = uniqueIdentifierService.createIdMapping(cloudId, providerId, recordId);
        } else {
            result = uniqueIdentifierService.createIdMapping(cloudId, providerId);
        }
        grantPermissionsToLocalId(result, providerId);

        return ResponseEntity.ok(result);
    }


    protected void grantPermissionsToLocalId(CloudId result, String providerId)
            throws NotFoundException, AlreadyExistsException {
        String creatorName = SpringUserUtils.getUsername();
        String key = result.getLocalId().getRecordId() + "/" + providerId;
        if (creatorName != null) {
            ObjectIdentity providerIdentity = new ObjectIdentityImpl(LOCAL_ID_CLASS_NAME, key);
            MutableAcl providerAcl = aclWrapper.createOrUpdateAcl(creatorName, providerIdentity);
            aclWrapper.updateAcl(providerAcl);
        }
    }
}
