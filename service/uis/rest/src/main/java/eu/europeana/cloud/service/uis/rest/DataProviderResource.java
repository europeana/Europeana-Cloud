package eu.europeana.cloud.service.uis.rest;

import static eu.europeana.cloud.common.web.ParamConstants.P_CLOUDID;
import static eu.europeana.cloud.common.web.ParamConstants.P_LOCALID;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Component;

import com.qmino.miredot.annotations.ReturnType;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.CloudId;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.UISParamConstants;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.UniqueIdentifierService;
import eu.europeana.cloud.service.uis.exception.CloudIdDoesNotExistException;
import eu.europeana.cloud.service.uis.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.uis.exception.IdHasBeenMappedException;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import eu.europeana.cloud.service.uis.exception.RecordDatasetEmptyException;
import eu.europeana.cloud.service.uis.exception.RecordIdDoesNotExistException;

/**
 * Resource for DataProvider.
 * 
 */
@Path("/data-providers")
@Component
@Scope("request")
public class DataProviderResource {

    @Autowired
    private UniqueIdentifierService uniqueIdentifierService;

    @Autowired
    private DataProviderService providerService;

    @PathParam(P_LOCALID)
    private String localId;

    @PathParam(P_CLOUDID)
    private String cloudId;

	@Autowired
	private MutableAclService mutableAclService;

	@Value("${numberOfElementsOnPage}")
	private int numberOfElementsOnPage;
	
	private final String DATA_PROVIDER_CLASS_NAME = DataProvider.class.getName(); 

    /**
     * Gets provider.
     * 
     * @return Data provider.
     * @throws ProviderDoesNotExistException
     */
    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	@Path("{providerId}")
    public DataProvider getProvider(@PathParam("providerId") String providerId)
            throws ProviderDoesNotExistException {
    	
        return providerService.getProvider(providerId);
    }


    /**
     * Updates data provider information. *
     * 
     * @param dataProviderProperties
     *            data provider properties.
     * @throws ProviderDoesNotExistException
     * @statuscode 204 object has been updated.
     */
    @PUT
	@Path("{providerId}")
    @Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @PreAuthorize("hasPermission(#providerId, 'eu.europeana.cloud.common.model.DataProvider', write)")
    public void updateProvider(@Context UriInfo uriInfo, 
    		DataProviderProperties dataProviderProperties, @PathParam("providerId") String providerId)
            throws ProviderDoesNotExistException {
        DataProvider provider = providerService.updateProvider(providerId, dataProviderProperties);
        EnrichUriUtil.enrich(uriInfo, provider);
    }

    /**
     * Get the record identifiers for a specific provider identifier with pagination
     * 
     * @param from
     * @param to
     * @return A list of record Identifiers (with their cloud identifiers)
     * @throws DatabaseConnectionException
     * @throws ProviderDoesNotExistException
     * @throws RecordDatasetEmptyException
     */
    @GET
	@Path("{id}/localIds")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    @ReturnType("eu.europeana.cloud.common.response.ResultSlice")
    public Response getLocalIdsByProvider(@PathParam("id") String providerId,
    			@QueryParam(UISParamConstants.Q_FROM) String from,
    			@QueryParam(UISParamConstants.Q_TO) @DefaultValue("10000") int to)
    				throws DatabaseConnectionException, ProviderDoesNotExistException, RecordDatasetEmptyException {
        ResultSlice<CloudId> pList = new ResultSlice<>();
        pList.setResults(uniqueIdentifierService.getLocalIdsByProvider(providerId, from, to));
        if (pList.getResults().size() == to) {
            pList.setNextSlice(pList.getResults().get(to - 1).getId());
        }
        return Response.ok(pList).build();

    }


    /**
     * Get the cloud identifiers for a specific provider identifier with pagination
     * 
     * @param from
     * @param to
     * @return A list of cloud Identifiers
     * @throws DatabaseConnectionException
     * @throws ProviderDoesNotExistException
     * @throws RecordDatasetEmptyException
     */
    @GET
    @Path("{id}/cloudIds")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    @ReturnType("eu.europeana.cloud.common.response.ResultSlice")
    public Response getCloudIdsByProvider(@PathParam("id") String providerId,
    			@QueryParam(UISParamConstants.Q_FROM) String from,
    			@QueryParam(UISParamConstants.Q_TO) @DefaultValue("10000") int to)
    					throws DatabaseConnectionException, ProviderDoesNotExistException, RecordDatasetEmptyException {
        ResultSlice<CloudId> pList = new ResultSlice<>();
        pList.setResults(uniqueIdentifierService.getCloudIdsByProvider(providerId, from, to));
        if (pList.getResults().size() == to) {
            pList.setNextSlice(pList.getResults().get(to - 1).getId());
        }
        return Response.ok(pList).build();
    }


    /**
     * Create a mapping between a cloud identifier and a record identifier for a provider
     * @param localId
     * @return The newly associated cloud identifier
     * @throws DatabaseConnectionException
     * @throws CloudIdDoesNotExistException
     * @throws IdHasBeenMappedException
     * @throws ProviderDoesNotExistException
     * @throws RecordDatasetEmptyException
     */
    @POST
    @Path("{id}/cloudIds/{" + P_CLOUDID + "}")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    public Response createIdMapping(@PathParam("id") String providerId, 
    		@QueryParam(UISParamConstants.Q_RECORD_ID) String localId)
            throws DatabaseConnectionException, CloudIdDoesNotExistException, IdHasBeenMappedException,
            ProviderDoesNotExistException, RecordDatasetEmptyException {
        if (localId != null) {
            return Response.ok().entity(uniqueIdentifierService.createIdMapping(cloudId, providerId, localId)).build();
        } else {
            return Response.ok().entity(uniqueIdentifierService.createIdMapping(cloudId, providerId)).build();
        }
    }


    /**
     * Remove the mapping between a record identifier and a cloud identifier
     * 
     * @return Confirmation that the mapping has been removed
     * @throws DatabaseConnectionException
     * @throws ProviderDoesNotExistException
     * @throws RecordIdDoesNotExistException
     */
    @DELETE
    @Path("{id}/localIds/{" + P_LOCALID + "}")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML })
    public Response removeIdMapping(@PathParam("id") String providerId)
            throws DatabaseConnectionException, ProviderDoesNotExistException, RecordIdDoesNotExistException {
        uniqueIdentifierService.removeIdMapping(providerId, localId);
        return Response.ok("Mapping marked as deleted").build();
    }
    
	/**
	 * Lists all providers. Result is returned in slices.
	 * 
	 * @param startFrom
	 *            reference to next slice of result.
	 * @return slice of result.
	 */
	@GET
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	public ResultSlice<DataProvider> getProviders(@QueryParam(UISParamConstants.Q_FROM) String startFrom) {
		return providerService.getProviders(startFrom, numberOfElementsOnPage);
	}
	
	/**
	 * Creates a new data provider. Response contains uri to created resource in
	 * as content location.
	 * 
	 * @param dataProviderProperties
	 *            data provider properties.
	 * @param providerId
	 *            data provider id (required)
	 * @return URI to created resource in content location
	 * @throws ProviderAlreadyExistsException
	 *             provider already * exists.
	 * @statuscode 201 object has been created.
	 */
	@POST
	@Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	@Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
	@PreAuthorize("isAuthenticated()")
	public Response createProvider(@Context UriInfo uriInfo, 
			DataProviderProperties dataProviderProperties,
			@QueryParam(UISParamConstants.Q_PROVIDER) String providerId) throws ProviderAlreadyExistsException {

		DataProvider provider = providerService.createProvider(providerId, dataProviderProperties);
		EnrichUriUtil.enrich(uriInfo, provider);
		
		// provider created => let's assign permissions to the owner
        String creatorName = getUsername(); 
        ObjectIdentity providerIdentity = new ObjectIdentityImpl(DATA_PROVIDER_CLASS_NAME, providerId);
        
		MutableAcl providerAcl = mutableAclService.createAcl(providerIdentity);

		providerAcl.insertAce(0, BasePermission.READ, new PrincipalSid(creatorName), true);
		providerAcl.insertAce(1, BasePermission.WRITE, new PrincipalSid(creatorName), true);
		providerAcl.insertAce(2, BasePermission.DELETE, new PrincipalSid(creatorName), true);
		providerAcl.insertAce(3, BasePermission.ADMINISTRATION, new PrincipalSid(creatorName), true);
		
		mutableAclService.updateAcl(providerAcl);
    	
		return Response.created(provider.getUri()).build();
	}
	
	/**
	 * @return Name of the currently logged in user
	 */
    private String getUsername() {
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();

        if (auth.getPrincipal() instanceof UserDetails) {
            return ((UserDetails) auth.getPrincipal()).getUsername();
        } else {
            return auth.getPrincipal().toString();
        }
    }
}
