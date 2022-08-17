package eu.europeana.cloud.service.mcs.utils;

import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.DataSetAssignmentException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.List;

/**
 * Verifies permissions to given dataset from given user
 */
public class DataSetPermissionsVerifier {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataSetPermissionsVerifier.class);

    private final DataSetService dataSetService;
    private final PermissionEvaluator permissionEvaluator;

    public DataSetPermissionsVerifier(DataSetService dataSetService, PermissionEvaluator permissionEvaluator) {
        this.dataSetService = dataSetService;
        this.permissionEvaluator = permissionEvaluator;
    }

    public boolean isUserAllowedToDelete(Representation representation) throws RepresentationNotExistsException, DataSetAssignmentException {
        return isPrivilegedUser() || hasDeletePermissionFor(representation);
    }

    public boolean isUserAllowedToPersistRepresentation(Representation representation) throws RepresentationNotExistsException, DataSetAssignmentException {
        return isPrivilegedUser() || hasWritePermissionFor(representation);
    }

    public boolean isUserAllowedToDeleteFileFor(Representation representation) throws RepresentationNotExistsException, DataSetAssignmentException {
        return isPrivilegedUser() || hasDeletePermissionFor(representation);
    }

    public boolean isUserAllowedToUploadFileFor(Representation representation) throws RepresentationNotExistsException, DataSetAssignmentException {
        return isPrivilegedUser() ||hasWritePermissionFor(representation);
    }

    public boolean isUserAllowedToAddRevisionTo(Representation representation) throws RepresentationNotExistsException, DataSetAssignmentException {
        return isPrivilegedUser() || hasWritePermissionFor(representation);
    }

    public boolean isUserAllowedToDeleteRevisionFor(Representation representation) throws RepresentationNotExistsException, DataSetAssignmentException {
        return isPrivilegedUser() || hasDeletePermissionFor(representation);
    }

    public boolean hasReadPermissionFor(Representation representation) throws RepresentationNotExistsException, DataSetAssignmentException {
        return hasPermissionFor(representation, Permission.READ);
    }

    public boolean hasWritePermissionFor(Representation representation) throws RepresentationNotExistsException, DataSetAssignmentException {
        return hasPermissionFor(representation, Permission.WRITE);
    }

    public boolean hasDeletePermissionFor(Representation representation) throws RepresentationNotExistsException, DataSetAssignmentException {
        return hasPermissionFor(representation, Permission.DELETE);
    }

    private boolean isPrivilegedUser() {
        SecurityContext ctx = SecurityContextHolder.getContext();
        Authentication authentication = ctx.getAuthentication();
        return authentication.getAuthorities().contains(new SimpleGrantedAuthority(Role.ADMIN.toString()))
                ||
                authentication.getAuthorities().contains(new SimpleGrantedAuthority(Role.EXECUTOR.toString()));
    }

    private boolean hasPermissionFor(Representation representation, Permission permission) throws DataSetAssignmentException, RepresentationNotExistsException {
        List<CompoundDataSetId> representationDataSets = dataSetService.getAllDatasetsForRepresentationVersion(representation);
        if (representationDataSets.size() != 1) {
            LOGGER.error("Representation has to be assigned to exactly one dataset. {}", representation.getCloudId());
            throw new DataSetAssignmentException("Representation assigned to more than one dataset. It is not allowed");
        } else {
            SecurityContext ctx = SecurityContextHolder.getContext();
            Authentication authentication = ctx.getAuthentication();
            //
            String targetId = representationDataSets.get(0).getDataSetId() + "/" + representationDataSets.get(0).getDataSetProviderId();
            return permissionEvaluator.hasPermission(authentication, targetId, DataSet.class.getName(), permission.getValue());
        }
    }

}
