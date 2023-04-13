package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.dps.controller.TopologyTasksResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.NotFoundException;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.stereotype.Service;

/**
 * @author krystian.
 */
@Service
public class PermissionManager {

  @Autowired
  private MutableAclService mutableAclService;

  /**
   * Grants permissions to the current user for the specified task.
   */
  public void grantPermissionsForTask(String taskId) {
    grantPermissionsForTask(taskId, SpringUserUtils.getUsername());
  }

  /**
   * Grants permissions for the specified task to the specified user.
   */
  public void grantPermissionsForTask(String taskId, String username) {
    MutableAcl taskAcl = null;
    ObjectIdentity taskObjectIdentity = new ObjectIdentityImpl(TopologyTasksResource.TASK_PREFIX, taskId);

    try {
      taskAcl = (MutableAcl) mutableAclService.readAclById(taskObjectIdentity);
    } catch (NotFoundException e) {
      taskAcl = mutableAclService.createAcl(taskObjectIdentity);
    }
    taskAcl.insertAce(taskAcl.getEntries().size(), BasePermission.WRITE, new PrincipalSid(username), true);
    taskAcl.insertAce(taskAcl.getEntries().size(), BasePermission.READ, new PrincipalSid(username), true);

    mutableAclService.updateAcl(taskAcl);
  }
}
