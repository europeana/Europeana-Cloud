package eu.europeana.cloud.service.mcs.utils;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Role;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.DataSetAssignmentException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

/**
 * Verifies permissions to given dataset from given user
 */
public class DataSetPermissionsVerifier {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataSetPermissionsVerifier.class);

  private final PermissionEvaluator permissionEvaluator;
  private final RecordService recordService;

  public DataSetPermissionsVerifier(PermissionEvaluator permissionEvaluator, RecordService recordService) {
    this.permissionEvaluator = permissionEvaluator;
    this.recordService = recordService;
  }

  public boolean isUserAllowedToDelete(Representation representation)
      throws RepresentationNotExistsException, DataSetAssignmentException {
    return isPrivilegedUser() || hasDeletePermissionFor(representation);
  }

  public boolean isUserAllowedToPersistRepresentation(Representation representation)
      throws RepresentationNotExistsException, DataSetAssignmentException {
    return isPrivilegedUser() || hasWritePermissionFor(representation);
  }

  public boolean isUserAllowedToDeleteFileFor(Representation representation)
      throws RepresentationNotExistsException, DataSetAssignmentException {
    return isPrivilegedUser() || hasDeletePermissionFor(representation);
  }

  public boolean isUserAllowedToUploadFileFor(Representation representation)
      throws RepresentationNotExistsException, DataSetAssignmentException {
    return isPrivilegedUser() || hasWritePermissionFor(representation);
  }

  public boolean isUserAllowedToAddRevisionTo(Representation representation)
      throws RepresentationNotExistsException, DataSetAssignmentException {
    return isPrivilegedUser() || hasWritePermissionFor(representation);
  }

  public boolean isUserAllowedToDeleteRevisionFor(Representation representation)
      throws RepresentationNotExistsException, DataSetAssignmentException {
    return isPrivilegedUser() || hasDeletePermissionFor(representation);
  }

  public boolean hasReadPermissionFor(Representation representation)
      throws RepresentationNotExistsException, DataSetAssignmentException {
    return hasPermissionFor(representation, Permission.READ);
  }

  public boolean hasWritePermissionFor(Representation representation)
      throws RepresentationNotExistsException, DataSetAssignmentException {
    return hasPermissionFor(representation, Permission.WRITE);
  }

  public boolean hasDeletePermissionFor(Representation representation)
      throws RepresentationNotExistsException, DataSetAssignmentException {
    return hasPermissionFor(representation, Permission.DELETE);
  }

  private boolean isPrivilegedUser() {
    SecurityContext ctx = SecurityContextHolder.getContext();
    Authentication authentication = ctx.getAuthentication();
    return authentication.getAuthorities().contains(new SimpleGrantedAuthority(Role.ADMIN))
        ||
        authentication.getAuthorities().contains(new SimpleGrantedAuthority(Role.EXECUTOR));
  }

  private boolean hasPermissionFor(Representation representation, Permission permission)
          throws RepresentationNotExistsException {
      representation = recordService.getRepresentation(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion());
      SecurityContext ctx = SecurityContextHolder.getContext();
      Authentication authentication = ctx.getAuthentication();
      String targetId = representation.getDatasetId() + "/" + representation.getDataProvider();
      return permissionEvaluator.hasPermission(authentication, targetId, DataSet.class.getName(), permission.getValue());
      }

}
