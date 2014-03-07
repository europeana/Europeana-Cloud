package eu.europeana.cloud.service.uis.rest;

import static eu.europeana.cloud.common.web.ParamConstants.P_LOCALID;
import static eu.europeana.cloud.common.web.ParamConstants.P_PROVIDER;

import javax.ws.rs.Consumes;
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
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.web.UISParamConstants;
import eu.europeana.cloud.service.uis.DataProviderService;
import eu.europeana.cloud.service.uis.exception.ProviderAlreadyExistsException;

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

	@Context
	private UriInfo uriInfo;
	
	@Value("${numberOfElementsOnPage}")
	private int numberOfElementsOnPage;

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
	 * as content location. * *
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
	public Response createProvider(DataProviderProperties dataProviderProperties,
			@QueryParam(UISParamConstants.Q_PROVIDER) String providerId) throws ProviderAlreadyExistsException {
		DataProvider provider = providerService.createProvider(providerId, dataProviderProperties);
		EnrichUriUtil.enrich(uriInfo, provider);
		return Response.created(provider.getUri()).build();
	}
}
