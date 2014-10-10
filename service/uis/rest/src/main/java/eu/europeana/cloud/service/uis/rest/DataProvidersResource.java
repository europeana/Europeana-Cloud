package eu.europeana.cloud.service.uis.rest;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.UISParamConstants;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;

/**
 * Resource for DataProviders.
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
    private MutableAclService mutableAclService;

    private final String DATA_PROVIDER_CLASS_NAME = DataProvider.class
	    .getName();

    /**
     * Lists all providers. Result is returned in slices.
     * 
     * @param startFrom
     *            reference to next slice of result.
     * @return slice of result.
     */
    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    public ResultSlice<DataProvider> getProviders(
	    @QueryParam(UISParamConstants.Q_FROM) String startFrom) {
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

	    MutableAcl providerAcl = mutableAclService
		    .createAcl(providerIdentity);

	    providerAcl.insertAce(0, BasePermission.READ, new PrincipalSid(
		    creatorName), true);
	    providerAcl.insertAce(1, BasePermission.WRITE, new PrincipalSid(
		    creatorName), true);
	    providerAcl.insertAce(2, BasePermission.DELETE, new PrincipalSid(
		    creatorName), true);
	    providerAcl.insertAce(3, BasePermission.ADMINISTRATION,
		    new PrincipalSid(creatorName), true);

	    mutableAclService.updateAcl(providerAcl);
	}

	return Response.created(provider.getUri()).build();
    }
}
