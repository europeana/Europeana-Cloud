package eu.europeana.cloud.service.uis.rest;

import com.qmino.miredot.annotations.ReturnType;
import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.UISParamConstants;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.uis.ACLServiceWrapper;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.exception.*;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.model.*;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.util.List;

import static eu.europeana.cloud.common.web.ParamConstants.*;

/**
 * Resource for DataProvider.
 *
 * @author
 */
@RestController
@RequestMapping("/data-providers/{" + P_PROVIDER + "}")
public class DataProviderResource {

    private UniqueIdentifierService uniqueIdentifierService;
    private DataProviderService providerService;
    private ACLServiceWrapper aclWrapper;

    public DataProviderResource(UniqueIdentifierService uniqueIdentifierService,
                                DataProviderService providerService,
                                ACLServiceWrapper aclWrapper) {
        this.uniqueIdentifierService = uniqueIdentifierService;
        this.providerService = providerService;
        this.aclWrapper = aclWrapper;
    }

    protected final String LOCAL_ID_CLASS_NAME = "LocalId";


    /**
     * Retrieves details about selected data provider
     *
     * @summary Data provider details retrieval
     *
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
    @GetMapping(produces = { MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE })
    public DataProvider getProvider(@PathVariable(P_PROVIDER) String providerId)
            throws ProviderDoesNotExistException {
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
     * @summary Data provider information update
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
    @PutMapping(consumes = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
    @PreAuthorize("hasPermission(#providerId, 'eu.europeana.cloud.common.model.DataProvider', write)")
    public ResponseEntity<Void> updateProvider(
            @RequestBody DataProviderProperties dataProviderProperties,
            @PathVariable(P_PROVIDER) String providerId,
            HttpServletRequest httpServletRequest)
            throws ProviderDoesNotExistException {

        providerService.updateProvider(providerId, dataProviderProperties);
        return ResponseEntity.noContent().build();
    }


    /**
     * Deletes data provider from database
     *
     * <br/>
     * <br/>
     * <div style='border-left: solid 5px #999999; border-radius: 10px; padding:
     * 6px;'> <strong>Required permissions:</strong>
     * <ul>
     * <li>Admin role</li>
     * </ul>
     * </div>
     *
     * @summary Data provider deletion
     *
     * @param dataProviderId
     *            <strong>REQUIRED</strong> data provider id
     *
     * @return Empty response with http status code indicating whether the
     *         operation was successful or not
     *
     * @throws ProviderDoesNotExistException
     *             The supplied provider does not exist
     *
     */
    @DeleteMapping
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    public ResponseEntity<Void> deleteProvider(@PathVariable(P_PROVIDER) String dataProviderId)
            throws ProviderDoesNotExistException {
        providerService.deleteProvider(dataProviderId);
        deleteProviderAcl(dataProviderId);
        return ResponseEntity.ok().build();
    }


    /**
     *
     * Get the local Identifiers (with their cloud identifiers) for a specific provider identifier with
     * pagination
     *
     * @summary Local identifiers retrieval.
     *
     * @param providerId
     *            <strong>REQUIRED</strong> identifier of provider for which all
     *            local identifiers will be retrieved
     * @param from
     *            from which one local identifier should we start.
     * @param to
     *            how many local identifiers should be contained in results
     *            list. Default is 10000
     *
     * @return A list of local Identifiers (with their cloud identifiers)
     *
     * @throws DatabaseConnectionException
     *             database error
     * @throws ProviderDoesNotExistException
     *             provider does not exist
     * @throws RecordDatasetEmptyException
     *             dataset is empty
     *
     */
    @GetMapping(path = "localIds", produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
    @ReturnType("eu.europeana.cloud.common.response.ResultSlice<eu.europeana.cloud.common.model.CloudId>")
    public ResponseEntity<ResultSlice<CloudId>> getLocalIdsByProvider(
            @PathVariable(P_PROVIDER) String providerId,
            @RequestParam(value = UISParamConstants.Q_FROM, required = false) String from,
            @RequestParam(value = UISParamConstants.Q_LIMIT, required = false, defaultValue = "10000") int to)
            throws DatabaseConnectionException, ProviderDoesNotExistException, RecordDatasetEmptyException {

        ResultSlice<CloudId> pList = new ResultSlice<>();
        pList.setResults(uniqueIdentifierService.getLocalIdsByProvider(providerId, from, to));
        if (pList.getResults().size() == to) {
            pList.setNextSlice(pList.getResults().get(to - 1).getId());
        }
        return ResponseEntity.ok(pList);
    }


    /**
     *
     * Get the cloud identifiers (with their local identifiers) for a specific provider identifier with
     * pagination
     *
     * @summary Cloud identifiers (with their local identifiers) retrieval.
     *
     * @param providerId
     *            <strong>REQUIRED</strong> identifier of provider for which all
     *            record identifiers will be retrieved
     * @param from
     *            from which one <strong>local identifier</strong> should we start.
     * @param limit
     *            how many cloud identifiers should be contained in results
     *            list. Default is 10000. <strong>Respected only if {@code from} parameter is defined.</strong>
     *
     * @return List of cloud identifiers (with their local identifiers) for specific provider
     *
     * @throws DatabaseConnectionException
     *             database connection errot
     * @throws ProviderDoesNotExistException
     *             provider does not exist
     * @throws RecordDatasetEmptyException
     *             record dataset is empty
     */
    @GetMapping(path = "cloudIds", produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE })
    @ReturnType("eu.europeana.cloud.common.response.ResultSlice<eu.europeana.cloud.common.model.CloudId>")
    public ResponseEntity<ResultSlice<CloudId>> getCloudIdsByProvider(
            @PathVariable(P_PROVIDER) String providerId,
            @RequestParam(value = UISParamConstants.Q_FROM, required = false) String from,
            @Min(0) @Max(10000) @RequestParam(value = UISParamConstants.Q_LIMIT, required = false, defaultValue = "10000") int limit)
            throws DatabaseConnectionException, ProviderDoesNotExistException, RecordDatasetEmptyException {
        ResultSlice<CloudId> slice = new ResultSlice<>();
        final int limitWithNextSlice = limit + 1;

        final List<CloudId> cloudIds = uniqueIdentifierService.getCloudIdsByProvider(providerId, from, limitWithNextSlice);

        if (cloudIds.size() == limitWithNextSlice) {
            setNextSliceAndRemoveLastElement(slice, limitWithNextSlice, cloudIds);
        }
        slice.setResults(cloudIds);
        return ResponseEntity.ok(slice);
    }

    private void setNextSliceAndRemoveLastElement(ResultSlice<CloudId> slice, int limitWithNextSlice, List<CloudId> cloudIdsByProvider) {
        CloudId nextSlice = cloudIdsByProvider.remove(limitWithNextSlice - 1);
        slice.setNextSlice(nextSlice.getLocalId().getRecordId());
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
     * @summary Cloud identifier to record identifier mapping creation
     *
     * @param providerId
     *            <strong>REQUIRED</strong> identifier of data provider, owner
     *            of the record
     * @param cloudId
     *            <strong>REQUIRED</strong> cloud identifier for which new
     *            record identifier will be added
     * @param localId
     *            record identifier which will be bound to selected cloud
     *            identifier. If not specified, random one will be generated
     *
     * @return The newly associated cloud identifier
     *
     * @throws DatabaseConnectionException
     *             database connection error
     * @throws CloudIdDoesNotExistException
     *             cloud identifier does not exist
     * @throws IdHasBeenMappedException
     *             identifier already mapped
     * @throws ProviderDoesNotExistException
     *             provider does not exist
     * @throws RecordDatasetEmptyException
     *             empty dataset
     * @throws CloudIdAlreadyExistException
     *             cloud identifier already exist
     *
     */
    @PostMapping(value = "cloudIds/{" + P_CLOUDID + "}",produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE })
    @PreAuthorize("isAuthenticated()")
    @ReturnType("eu.europeana.cloud.common.model.CloudId")
    public ResponseEntity<CloudId> createIdMapping(
            @PathVariable(P_PROVIDER) String providerId,
            @PathVariable(P_CLOUDID) String cloudId,
            @RequestParam(UISParamConstants.Q_RECORD_ID) String localId)
            throws DatabaseConnectionException, CloudIdDoesNotExistException, IdHasBeenMappedException,
            ProviderDoesNotExistException, RecordDatasetEmptyException, CloudIdAlreadyExistException {

        CloudId result = null;
        if (localId != null) {
            result = uniqueIdentifierService.createIdMapping(cloudId, providerId, localId);
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
            MutableAcl providerAcl = aclWrapper.getAcl(creatorName, providerIdentity);
            aclWrapper.updateAcl(providerAcl);
        }
    }


    /**
     *
     * Remove the mapping between a record identifier and a cloud identifier
     *
     * <br/>
     * <br/>
     * <div style='border-left: solid 5px #999999; border-radius: 10px; padding:
     * 6px;'> <strong>Required permissions:</strong>
     * <ul>
     * <li>Authenticated user</li>
     * <li>Write permissions for selected data provider and local identifier
     * (granted at the mapping creation)</li>
     * </ul>
     * </div>
     *
     * @summary Mapping between record and cloud identifier removal
     *
     * @param providerId
     *            <strong>REQUIRED</strong> identifier of the provider, owner of
     *            the record
     *
     * @param localId
     *            <strong>REQUIRED</strong> record identifier which will be
     *            detached from selected provider identifier.
     *
     * @return Confirmation that the mapping has been removed
     *
     * @throws DatabaseConnectionException
     *             database error
     * @throws ProviderDoesNotExistException
     *             provider does not exist
     * @throws RecordIdDoesNotExistException
     *             record does not exist
     */
    @DeleteMapping(value = "localIds/{" + P_LOCALID + "}", produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
    @PreAuthorize("hasPermission(#localId.concat('/').concat(#providerId),'" + LOCAL_ID_CLASS_NAME + "', write)")
    public ResponseEntity<String> removeIdMapping(@PathVariable(P_PROVIDER) String providerId, @PathVariable(P_LOCALID) String localId)
            throws DatabaseConnectionException, ProviderDoesNotExistException, RecordIdDoesNotExistException {
        uniqueIdentifierService.removeIdMapping(providerId, localId);
        deleteLocalIdAcl(localId, providerId);
        return ResponseEntity.ok("Mapping marked as deleted");
    }


    protected void deleteLocalIdAcl(String localId, String providerId)
            throws ChildrenExistException {
        String key = localId + "/" + providerId;
        ObjectIdentity providerIdentity = new ObjectIdentityImpl(LOCAL_ID_CLASS_NAME, key);
        aclWrapper.deleteAcl(providerIdentity, false);
    }


    private void deleteProviderAcl(String dataProviderId) {
        ObjectIdentity providerIdentity = new ObjectIdentityImpl(DataProvider.class.getName(), dataProviderId);
        aclWrapper.deleteAcl(providerIdentity, false);
    }
}
