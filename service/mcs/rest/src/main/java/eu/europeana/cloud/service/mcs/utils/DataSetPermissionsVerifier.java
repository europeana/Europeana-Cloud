package eu.europeana.cloud.service.mcs.utils;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Role;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

/**
 * Verifies permissions to given dataset from given user
 */
public class DataSetPermissionsVerifier {
  private final PermissionEvaluator permissionEvaluator;
  private final RecordService recordService;

  /**
   * Constructs DataSetPermissionsVerifier with given permission evaluator and record service.
   *
   * @param permissionEvaluator permission evaluator that verifies if user has given permission
   * @param recordService record service to retrieve representation
   */
  public DataSetPermissionsVerifier(PermissionEvaluator permissionEvaluator, RecordService recordService) {
    this.permissionEvaluator = permissionEvaluator;
    this.recordService = recordService;
  }

  public boolean isUserAllowedToDelete(Representation representation)
      throws RepresentationNotExistsException {
    return isPrivilegedUser() || hasDeletePermissionFor(representation);
  }

  public boolean isUserAllowedToPersistRepresentation(Representation representation)
      throws RepresentationNotExistsException {
    return isPrivilegedUser() || hasWritePermissionFor(representation);
  }

  public boolean isUserAllowedToDeleteFileFor(Representation representation)
      throws RepresentationNotExistsException {
    return isPrivilegedUser() || hasDeletePermissionFor(representation);
  }

  public boolean isUserAllowedToUploadFileFor(Representation representation)
      throws RepresentationNotExistsException {
    return isPrivilegedUser() || hasWritePermissionFor(representation);
  }

  /**
   * Verifies is given user has privileges to read {@link Representation}
   *
   * @param representation {@link Representation} that is about to be read
   *
   * @return true of false indicating that given user is/isn't allowed to read {@link Representation}
   * @throws RepresentationNotExistsException in case of non-existing representation
   */
  public boolean hasReadPermissionFor(Representation representation)
      throws RepresentationNotExistsException {
    return hasPermissionFor(representation, Permission.READ);
  }

  public boolean hasWritePermissionFor(Representation representation)
      throws RepresentationNotExistsException {
    return hasPermissionFor(representation, Permission.WRITE);
  }

  public boolean hasDeletePermissionFor(Representation representation)
      throws RepresentationNotExistsException {
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
    representation = recordService.getRepresentation(representation.getCloudId(), representation.getRepresentationName(),
        representation.getVersion());

    if (representation.getDatasetIds().isEmpty()) {
      throw new RuntimeException("Representation is not assigned to any dataset: " + representation);
    }

    for (String datasetId : representation.getDatasetIds()) {
      if (!hasPermissionForDataset(representation.getDataProvider(), datasetId, permission)) {
        return false;
      }
    }
    return true;

  }

  public boolean hasWritePermissionForDataset(String providerId,String datasetId) {
    return hasPermissionForDataset(providerId, datasetId, Permission.WRITE);
  }

  private boolean hasPermissionForDataset(String providerId,String datasetId, Permission permission) {
    SecurityContext ctx = SecurityContextHolder.getContext();
    Authentication authentication = ctx.getAuthentication();
    String targetId = datasetId + "/" + providerId;
    return permissionEvaluator.hasPermission(authentication, targetId, DataSet.class.getName(), permission.getValue());
  }
}
