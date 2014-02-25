package eu.europeana.cloud.service.mcs.rest;

import static eu.europeana.cloud.common.web.ParamConstants.F_PROVIDER;
import static eu.europeana.cloud.common.web.ParamConstants.P_GID;
import static eu.europeana.cloud.common.web.ParamConstants.P_REPRESENTATIONNAME;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;

/**
 * Resource to manage representations.
 */
@Path("/records/{" + P_GID + "}/representations/{" + P_REPRESENTATIONNAME + "}")
@Component
@Scope("request")
public class RepresentationResource {

    @Autowired
    private RecordService recordService;

    @Context
    private UriInfo uriInfo;

    @PathParam(P_GID)
    private String globalId;

    @PathParam(P_REPRESENTATIONNAME)
    private String schema;


    /**
     * Returns latest persistent version of representation.
     * 
     * @return requested representation in latest persistent version.
     * @throws RepresentationNotExistsException
     *             representation does not exist or no persistent version of this representation exists.
     */
    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    public Representation getRepresentation()
            throws RepresentationNotExistsException {
        Representation info = recordService.getRepresentation(globalId, schema);
        prepare(info);
        return info;
    }


    /**
     * Deletes representation with all versions.
     * 
     * @throws RepresentationNotExistsException
     *             Representation does not exist.
     */
    @DELETE
    public void deleteRepresentation()
            throws RepresentationNotExistsException {
        recordService.deleteRepresentation(globalId, schema);
    }


    /**
     * Creates new representation version. Url of created representation version will be returned in response.
     * 
     * @param providerId
     *            provider of this representation versio.
     * @return created representation.
     * @throws RecordNotExistsException
     *             provided id is not known to Unique Identifier Service.
     * @throws ProviderNotExistsException
     *             no provider with given id exists
     * @statuscode 201 object has been created.
     * */
    @POST
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response createRepresentation(@FormParam(F_PROVIDER) String providerId)
            throws RecordNotExistsException, ProviderNotExistsException {
        ParamUtil.require(F_PROVIDER, providerId);
        Representation version = recordService.createRepresentation(globalId, schema, providerId);
        EnrichUriUtil.enrich(uriInfo, version);
        return Response.created(version.getUri()).build();
    }


    private void prepare(Representation representation) {
        representation.setFiles(null);
        EnrichUriUtil.enrich(uriInfo, representation);
    }
}
