package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.model.DataProviderProperties;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.DataProviderService;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.F_PROVIDER;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.F_START_FROM;
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
import org.springframework.stereotype.Component;

/**
 * Resource for DataProviders
 *
 */
@Path("/data-providers")
@Component
public class DataProvidersResource {

	@Autowired
	private DataProviderService providerService;

	@Context
	private UriInfo uriInfo;


	@GET
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public ResultSlice<DataProvider> getProviders(
			@QueryParam(F_START_FROM) String startFrom) {
		return providerService.getProviders(startFrom, ParamUtil.numberOfElements());
	}


	@POST
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	@Consumes({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public Response updateProvider(DataProviderProperties dataProviderProperties,
			@QueryParam(F_PROVIDER) String providerId) {
		DataProvider provider = providerService.createProvider(providerId, dataProviderProperties);
		EnrichUriUtil.enrich(uriInfo, provider);
		return Response.created(provider.getUri()).build();
	}
}
