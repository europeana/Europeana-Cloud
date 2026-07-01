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

  /**
   * Verifies is given user has privileges to delete {@link Representation}
   *
   * @param representation {@link Representation} that is about to be deleted
   *
   * @return true of false indicating that given user is/isn't allowed to delete {@link Representation}
   * @throws RepresentationNotExistsException in case of non-existing representation
   */
  public boolean isUserAllowedToDelete(Representation representation)
      throws RepresentationNotExistsException {
    return isPrivilegedUser() || hasDeletePermissionFor(representation);
  }

  /**
   * Verifies is given user has privileges to persist {@link Representation}
   *
   * @param representation {@link Representation} that is about to be persisted
   *
   * @return true of false indicating that given user is/isn't allowed to persist {@link Representation}
   * @throws RepresentationNotExistsException in case of non-existing representation
   */
  public boolean isUserAllowedToPersistRepresentation(Representation representation)
      throws RepresentationNotExistsException {
    return isPrivilegedUser() || hasWritePermissionFor(representation);
  }

  /**
   * Verifies is given user has privileges to delete {@link eu.europeana.cloud.common.model.File} for {@link Representation}
   *
   * @param representation {@link Representation} that is about to be modified
   *
   * @return true of false indicating that given user is/isn't allowed to delete {@link eu.europeana.cloud.common.model.File} for {@link Representation}
   * @throws RepresentationNotExistsException in case of non-existing representation
   */
  public boolean isUserAllowedToDeleteFileFor(Representation representation)
      throws RepresentationNotExistsException {
    return isPrivilegedUser() || hasDeletePermissionFor(representation);
  }

  /**
   * Verifies is given user has privileges to upload {@link eu.europeana.cloud.common.model.File} for {@link Representation}
   *
   * @param representation {@link Representation} that is about to be modified
   *
   * @return true of false indicating that given user is/isn't allowed to upload {@link eu.europeana.cloud.common.model.File} for {@link Representation}
   * @throws RepresentationNotExistsException in case of non-existing representation
   */
  public boolean isUserAllowedToUploadFileFor(Representation representation)
      throws RepresentationNotExistsException {
    return isPrivilegedUser() || hasWritePermissionFor(representation);
  }

  /**
   * Verifies is given user has privileges to add {@link eu.europeana.cloud.common.model.Revision} for {@link Representation}
   *
   * @param representation {@link Representation} that is about to be modified
   *
   * @return true of false indicating that given user is/isn't allowed to add {@link eu.europeana.cloud.common.model.Revision} for {@link Representation}
   * @throws RepresentationNotExistsException in case of non-existing representation
   */
  public boolean isUserAllowedToAddRevisionTo(Representation representation)
      throws RepresentationNotExistsException {
    return isPrivilegedUser() || hasWritePermissionFor(representation);
  }

  /**
   * Verifies is given user has privileges to add annotation to given representation
   *
   * @param representation representation to which the annotation will be added
   * @return true of false indicating that given user is/isn't allowed to add annotation to representation
   *
   * @throws RepresentationNotExistsException in case of non-existing representation
   */
  public boolean isUserAllowedToAddAnnotationTo(Representation representation)
          throws RepresentationNotExistsException {
    return isPrivilegedUser() || hasWritePermissionFor(representation);
  }

  /**
   * Verifies is given user has privileges to delete {@link eu.europeana.cloud.common.model.Revision} for {@link Representation}
   *
   * @param representation {@link Representation} that is about to be modified
   *
   * @return true of false indicating that given user is/isn't allowed to delete {@link eu.europeana.cloud.common.model.Revision} for {@link Representation}
   * @throws RepresentationNotExistsException in case of non-existing representation
   */

  public boolean isUserAllowedToDeleteRevisionFor(Representation representation)
      throws RepresentationNotExistsException {
    return isPrivilegedUser() || hasDeletePermissionFor(representation);
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

  /**
   * Verifies if given user has {@link Permission#WRITE} to the given {@link Representation}
   * @param representation {@link Representation} to which the revision will be added
   * @return <b>true</b> if user has {@link Permission#WRITE} to {@link Representation}, <b>false</b> otherwise
   * @throws RepresentationNotExistsException in case of non-existing {@link Representation}
   */
  public boolean hasWritePermissionFor(Representation representation)
      throws RepresentationNotExistsException {
    return hasPermissionFor(representation, Permission.WRITE);
  }

  /**
   * Verifies is given user has privileges to delete {@link Representation}
   *
   * @param representation {@link Representation} that is about to be deleted
   *
   * @return true of false indicating that given user is/isn't allowed to delete {@link Representation}
   * @throws RepresentationNotExistsException in case of non-existing representation
   */
  public boolean hasDeletePermissionFor(Representation representation)
      throws RepresentationNotExistsException {
    return hasPermissionFor(representation, Permission.DELETE);
  }

  /**
   * Verifies is given user has {@link Role#ADMIN} or {@link Role#EXECUTOR} role
   *
   * @return <b>true</b> if user has {@link Role#ADMIN} or {@link Role#EXECUTOR} role, <b>false</b> otherwise
   */
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
