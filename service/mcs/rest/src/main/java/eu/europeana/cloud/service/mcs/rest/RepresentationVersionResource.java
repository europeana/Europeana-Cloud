package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.List;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATION_VERSION;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATION_VERSION_PERSIST;

/**
 * Resource to manage representation versions.
 */
@RestController
public class RepresentationVersionResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(RepresentationVersionResource.class.getName());

    private final RecordService recordService;
    private final DataSetService dataSetService;
    private final PermissionEvaluator permissionEvaluator;

    public RepresentationVersionResource(
            RecordService recordService,
            DataSetService dataSetService,
            PermissionEvaluator permissionEvaluator) {
        this.recordService = recordService;
        this.dataSetService = dataSetService;
        this.permissionEvaluator = permissionEvaluator;
    }

    /**
     * Returns representation in a specified version.
     * <strong>Read permissions required.</strong>
     *
     * @param cloudId cloud id of the record which contains the representation(required).
     * @param representationName   name of the representation(required).
     * @param version  a specific version of the representation(required).
     * @return representation in requested version
     * @throws RepresentationNotExistsException representation does not exist in the
     *                                          specified version.
     * @summary get representation by version
     */
    @GetMapping(value = REPRESENTATION_VERSION, produces = {MediaType.APPLICATION_XML_VALUE, MediaType.APPLICATION_JSON_VALUE})
    @PreAuthorize("isAuthenticated()")
    public @ResponseBody Representation getRepresentationVersion(
            HttpServletRequest httpServletRequest,
            @PathVariable String cloudId,
            @PathVariable String representationName,
            @PathVariable String version) throws RepresentationNotExistsException {

        Representation representation = recordService.getRepresentation(cloudId, representationName, version);
        EnrichUriUtil.enrich(httpServletRequest, representation);
        return representation;
    }

    /**
     * Deletes representation version.
     * <strong>Delete permissions required.</strong>
     *
     * @param cloudId cloud id of the record which contains the representation version (required).
     * @param representationName   name of the representation(required).
     * @param version  a specific version of the representation(required).
     * @throws RepresentationNotExistsException              representation does not exist in
     *                                                       specified version.
     * @throws CannotModifyPersistentRepresentationException representation in
     *                                                       specified version is persistent and as such cannot be removed.
     */
    @DeleteMapping(value = REPRESENTATION_VERSION)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteRepresentation(
            @PathVariable String cloudId,
            @PathVariable String representationName,
            @PathVariable String version) throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException, AccessDeniedOrObjectDoesNotExistException {

        Representation representation = buildRepresentationFromRequestParameters(cloudId, representationName, version);

        if (isUserAllowedToDelete(representation)) {
            recordService.deleteRepresentation(cloudId, representationName, version);
        }else{
            throw new AccessDeniedOrObjectDoesNotExistException();
        }
    }

    /**
     * Persists temporary representation.
     * <p/>
     * <strong>Write permissions required.</strong>
     *
     * @param cloudId cloud id of the record which contains the representation version(required).
     * @param representationName   name of the representation(required).
     * @param version  a specific version of the representation(required).
     * @return URI to the persisted representation in content-location.
     * @throws RepresentationNotExistsException              representation does not exist in
     *                                                       specified version.
     * @throws CannotModifyPersistentRepresentationException representation
     *                                                       version is already persistent.
     * @throws CannotPersistEmptyRepresentationException     representation version
     *                                                       has no file attached and as such cannot be made persistent.
     * @statuscode 201 representation is made persistent.
     */
    @PostMapping(value = REPRESENTATION_VERSION_PERSIST)
    public ResponseEntity<Void> persistRepresentation(
            HttpServletRequest httpServletRequest,
            @PathVariable String cloudId,
            @PathVariable String representationName,
            @PathVariable String version) throws RepresentationNotExistsException,
            CannotModifyPersistentRepresentationException, CannotPersistEmptyRepresentationException, AccessDeniedOrObjectDoesNotExistException {

        Representation representation = buildRepresentationFromRequestParameters(cloudId, representationName, version);

        if (isUserAllowedToPersistRepresentation(representation)) {
            Representation persistentRepresentation = recordService.persistRepresentation(cloudId, representationName, version);
            EnrichUriUtil.enrich(httpServletRequest, persistentRepresentation);
            return ResponseEntity.created(persistentRepresentation.getUri()).build();
        }else{
            throw new AccessDeniedOrObjectDoesNotExistException();
        }

    }

    private Representation buildRepresentationFromRequestParameters(String cloudId, String representationName, String version) {
        Representation representation = new Representation();
        representation.setCloudId(cloudId);
        representation.setRepresentationName(representationName);
        representation.setVersion(version);
        return representation;
    }

    private boolean isUserAllowedToDelete(Representation representation) throws RepresentationNotExistsException {
        List<CompoundDataSetId> representationDataSets = dataSetService.getAllDatasetsForRepresentationVersion(representation);
        if (representationDataSets.size() != 1) {
            LOGGER.error("Should never happen");
        } else {
            SecurityContext ctx = SecurityContextHolder.getContext();
            Authentication authentication = ctx.getAuthentication();
            //
            String targetId = representationDataSets.get(0).getDataSetId() + "/" + representationDataSets.get(0).getDataSetProviderId();
            return permissionEvaluator.hasPermission(authentication, targetId, DataSet.class.getName(), "read");
        }
        return false;
    }

    private boolean isUserAllowedToPersistRepresentation(Representation representation) throws RepresentationNotExistsException {
        return isUserAllowedToDelete(representation);

    }

}
