package eu.europeana.cloud.service.uis.rest;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.UISParamConstants;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.uis.ACLServiceWrapper;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.stereotype.Component;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

/**
 * Resource for DataProviders.
 * 
 * @author
 * 
 */
@Path("/data-providers")
@Component
@Scope("request")
public class DataProvidersResource {

    @Autowired
    private DataProviderService providerService;

    @Value("${numberOfElementsOnPage}")
    private int numberOfElementsOnPage;

    @Autowired
    private ACLServiceWrapper aclWrapper;

    private final String DATA_PROVIDER_CLASS_NAME = DataProvider.class
	    .getName();

    /**
     * Lists all providers stored in eCloud. Result is returned in slices.
     * 
	 * @summary All providers list
	 * 
     * @param startFrom
     *            data provider identifier from which returned slice of results will be generated. 
	 *            If not provided then result list will contain data providers from the first one.
	 *            
     * @return one slice of result containing eCloud data providers.
     */
    @GET
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public ResultSlice<DataProvider> getProviders(
			@QueryParam(UISParamConstants.Q_FROM) String startFrom) {
		return providerService.getProviders(startFrom, numberOfElementsOnPage);
	}

    /**
     * Creates a new data provider. Response contains uri to created resource in
     * as content location.
     * 
	 * @summary Data provider creation
	 * 
     * @param dataProviderProperties
     *            <strong>REQUIRED</strong> data provider properties.
     * @param providerId
     *            <strong>REQUIRED</strong> data provider identifier for newly created provider
     * @return URI to created resource in content location
     * @throws ProviderAlreadyExistsException
     *             provider already exists.
     * @statuscode 201 new provider has been created.
	 * @statuscode 400 request body cannot be is empty
     */
    @POST
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @Consumes({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    @PreAuthorize("isAuthenticated()")
    public Response createProvider(@Context UriInfo uriInfo,
	    DataProviderProperties dataProviderProperties,
	    @QueryParam(UISParamConstants.Q_PROVIDER) String providerId)
	    throws ProviderAlreadyExistsException {
	DataProvider provider = providerService.createProvider(providerId,
		dataProviderProperties);
	EnrichUriUtil.enrich(uriInfo, provider);

	// provider created => let's assign permissions to the owner
	String creatorName = SpringUserUtils.getUsername();
	if (creatorName != null) {
	    ObjectIdentity providerIdentity = new ObjectIdentityImpl(
		    DATA_PROVIDER_CLASS_NAME, providerId);

	    MutableAcl providerAcl = aclWrapper.getAcl(creatorName, providerIdentity);
	    aclWrapper.updateAcl(providerAcl);
	}

	return Response.created(provider.getUri()).build();
    }
}
