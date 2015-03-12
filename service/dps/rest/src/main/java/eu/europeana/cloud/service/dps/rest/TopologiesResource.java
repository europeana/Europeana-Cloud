package eu.europeana.cloud.service.dps.rest;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.springframework.security.acls.model.MutableAclService;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.service.dps.DpsService;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.ObjectIdentity;

/**
 * Resource to fetch / submit Tasks to the DPS service
 * 
 */
@Path("/topologies")
@Component
@Scope("request")
public class TopologiesResource {

	@Autowired
	private DpsService dps;
	
	@Autowired
        private MutableAclService mutableAclService;

        private final static String TOPOLOGY_PREFIX = "Topology";
        
        
        @POST
        @PreAuthorize("hasRole('ROLE_ADMIN')")
        @Consumes({MediaType.APPLICATION_FORM_URLENCODED})
        public Response assignPersmissionsToTopology(@PathParam("user") String userName, @PathParam("topologyName") String topology) {

            ObjectIdentity topologyIdentity = new ObjectIdentityImpl(TOPOLOGY_PREFIX,
                    topology);

            MutableAcl topologyAcl = mutableAclService.createAcl(topologyIdentity);

            topologyAcl.insertAce(0, BasePermission.WRITE, new PrincipalSid(userName), true);
            mutableAclService.updateAcl(topologyAcl);

            return Response.ok().build();
        }
        
 
}
