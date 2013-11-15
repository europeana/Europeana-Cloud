package eu.europeana.cloud.service.mcs.rest;

import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.service.mcs.DataProviderService;
import eu.europeana.cloud.common.response.ResultSlice;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.*;
import javax.ws.rs.QueryParam;

/**
 * Resource for DataProviders
 *
 */
@Path("/data-providers")
@Component
public class DataProvidersResource {

	@Autowired
	private DataProviderService providerService;


	@GET
	@Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
	public ResultSlice<DataProvider> getProviders(
			@QueryParam(F_START_FROM) String startFrom,
			@QueryParam(F_LIMIT) Integer limit) {
		if (limit == null) {
			limit = 100;
		}
		return providerService.getProviders(startFrom, limit);
	}
}
