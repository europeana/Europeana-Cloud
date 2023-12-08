package eu.europeana.cloud.service.dps.controller;

import eu.europeana.cloud.service.dps.exception.AccessDeniedOrTopologyDoesNotExistException;
import eu.europeana.cloud.service.dps.service.utils.TopologyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import static eu.europeana.metis.utils.CommonStringValues.CRLF_PATTERN;

/**
 * Resource to manage topologies in the DPS service
 */
@RestController
@RequestMapping("/{topologyName}")
public class TopologiesResource {

  private static final String TOPOLOGY_PREFIX = "Topology";
  private static final Logger LOGGER = LoggerFactory.getLogger(TopologiesResource.class);
  private final MutableAclService mutableAclService;
  private final TopologyManager topologyManager;

  public TopologiesResource(MutableAclService mutableAclService, TopologyManager topologyManager) {
    this.mutableAclService = mutableAclService;
    this.topologyManager = topologyManager;
  }

  /**
   * Grants user with given username read/ write permissions for the requested topology.
   * <p>
   * <br/><br/>
   * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
   * 		<strong>Required permissions:</strong>
   * 			<ul>
   *     			<li>Admin role</li>
   * 			</ul>
   * </div>
   *
   * @param topology <strong>REQUIRED</strong> Name of the topology.
   * @param userName <strong>REQUIRED</strong> Permissions are granted to the account with this unique username
   * @return Empty response with status code indicating whether the operation was successful or not.
   * @summary Grant topology permissions
   */
  @PostMapping(path = "/permit", consumes = {MediaType.APPLICATION_FORM_URLENCODED_VALUE})
  @PreAuthorize("hasRole('ROLE_ADMIN')")
  public ResponseEntity<Void> grantPermissionsToTopology(
      @RequestParam("username") String userName,
      @PathVariable("topologyName") String topology) throws AccessDeniedOrTopologyDoesNotExistException {

    if (LOGGER.isInfoEnabled()) {
      LOGGER.info("Adding permissions for user '{}' to topology: '{}'",
              CRLF_PATTERN.matcher(userName).replaceAll(""),
              CRLF_PATTERN.matcher(topology).replaceAll(""));
    }

    assertContainTopology(topology);
    addPermissionsToTopology(topology, userName);
    return ResponseEntity.ok().build();
  }

  private void assertContainTopology(String topology) throws AccessDeniedOrTopologyDoesNotExistException {
    if (!topologyManager.containsTopology(topology)) {
      throw new AccessDeniedOrTopologyDoesNotExistException();
    }
  }

  private void addPermissionsToTopology(String topology, String userName) {
    ObjectIdentity topologyIdentity = new ObjectIdentityImpl(TOPOLOGY_PREFIX, topology);
    MutableAcl topologyAcl;

    try {
      topologyAcl = (MutableAcl) mutableAclService.readAclById(topologyIdentity);

    } catch (Exception e) {
      // not really an exception
      LOGGER.info("ACL not found for topology {} and user {}. "
          + "This is ok if it is the first time you are trying to assign permissions for this topology.", topology, userName);
      topologyAcl = mutableAclService.createAcl(topologyIdentity);
    }

    topologyAcl.insertAce(topologyAcl.getEntries().size(), BasePermission.WRITE, new PrincipalSid(userName), true);
    mutableAclService.updateAcl(topologyAcl);
  }
}
