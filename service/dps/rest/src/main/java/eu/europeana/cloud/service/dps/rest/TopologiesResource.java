package eu.europeana.cloud.service.dps.rest;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.stereotype.Component;

import com.qmino.miredot.annotations.ReturnType;

import eu.europeana.cloud.service.dps.TaskExecutionReportService;

/**
 * Resource to manage topologies in the DPS service
 */
@Path("/topologies/{topologyName}")
@Component
public class TopologiesResource {

    @Autowired
    private TaskExecutionReportService dps;

    @Autowired
    private MutableAclService mutableAclService;

    private final static String TOPOLOGY_PREFIX = "Topology";

    private static final Logger LOGGER = LoggerFactory.getLogger(TopologiesResource.class);

    /**
     * Grants user with given username read/ write permissions for the requested topology.
     *
     * <br/><br/>
     * <div style='border-left: solid 5px #999999; border-radius: 10px; padding: 6px;'>
     * 		<strong>Required permissions:</strong>
     * 			<ul>
     *     			<li>Admin role</li>
     * 			</ul>
     * </div>
     *
     * @summary Grant topology permissions
     * @param topology <strong>REQUIRED</strong> Name of the topology.
     * @param userName <strong>REQUIRED</strong> Permissions are granted to the account with this unique username
     *
     * @return Empty response with status code indicating whether the operation was successful or not.
     */
    @Path("/permit")
    @POST
    @PreAuthorize("hasRole('ROLE_ADMIN')")
    @Consumes({MediaType.APPLICATION_FORM_URLENCODED})
    public Response grantPermissionsToTopology(@FormParam("user") String userName, @PathParam("topologyName") String topology) {
        
        ObjectIdentity topologyIdentity = new ObjectIdentityImpl(TOPOLOGY_PREFIX,
                topology);
        //
        MutableAcl topologyAcl = null;
        try {
            topologyAcl = (MutableAcl)mutableAclService.readAclById(topologyIdentity);
        } catch (Exception e) {
            LOGGER.warn("ACL not found for topology {} and user {}", topology, userName);
            topologyAcl = mutableAclService.createAcl(topologyIdentity);
        }
        topologyAcl.insertAce(0, BasePermission.WRITE, new PrincipalSid(userName), true);
        mutableAclService.updateAcl(topologyAcl);

        return Response.ok().build();
    }
}
