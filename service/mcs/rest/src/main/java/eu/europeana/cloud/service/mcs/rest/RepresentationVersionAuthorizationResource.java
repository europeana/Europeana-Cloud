package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.Representation;
import static eu.europeana.cloud.common.web.AASParamConstants.Q_USER_NAME;
import static eu.europeana.cloud.common.web.ParamConstants.P_CLOUDID;
import static eu.europeana.cloud.common.web.ParamConstants.P_REPRESENTATIONNAME;
import static eu.europeana.cloud.common.web.ParamConstants.P_VER;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import javax.ws.rs.Consumes;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.stereotype.Component;

/**
 * Resource to authorize other users to read specific versions.
 *
 * @author Markus Muhr (markus.muhr@theeuropeanlibrary.org)
 * @since 22.08.2014
 */
@Path("/records/{" + P_CLOUDID + "}/representations/{" + P_REPRESENTATIONNAME + "}/versions/{" + P_VER + "}/users/{" + Q_USER_NAME + "}")
@Component
@Scope("request")
public class RepresentationVersionAuthorizationResource {

    @PathParam(P_CLOUDID)
    private String globalId;

    @PathParam(P_REPRESENTATIONNAME)
    private String schema;

    @PathParam(P_VER)
    private String version;

    @PathParam(Q_USER_NAME)
    private String username;

    @Autowired
    private MutableAclService mutableAclService;

    private final String REPRESENTATION_CLASS_NAME = Representation.class.getName();

    /**
     * Modify authorization of versions operation. Updates authorization. for a
     * specific representation version.
     *
     * @return response tells you if authorization has been updated or not
     * @statuscode 204 object has been updated.
     */
    @PUT
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @PreAuthorize("hasPermission(#globalId.concat('/').concat(#schema).concat('/').concat(#version), 'eu.europeana.cloud.common.model.Representation', write)")
    public Response updateAuthorization() {
        ObjectIdentity versionIdentity = new ObjectIdentityImpl(REPRESENTATION_CLASS_NAME,
                globalId + "/" + schema + "/" + version);

        MutableAcl versionAcl = (MutableAcl) mutableAclService.readAclById(versionIdentity);

        if (versionAcl != null) {
            versionAcl.insertAce(0, BasePermission.READ, new PrincipalSid(username), true);
            mutableAclService.updateAcl(versionAcl);
            return Response.ok("Authorization has been updated!").build();
        } else {
            return Response.notModified("Authorization has NOT been updated!").build();
        }
    }
}
