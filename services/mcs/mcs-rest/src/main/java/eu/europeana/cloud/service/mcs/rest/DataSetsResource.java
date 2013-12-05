package eu.europeana.cloud.service.mcs.rest;

import static eu.europeana.cloud.service.mcs.rest.ParamConstants.*;

import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.DataProviderService;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;

@Path("/data-providers/{" + P_PROVIDER + "}/data-sets")
@Component
public class DataSetsResource {

    private static final Logger log = LoggerFactory.getLogger(DataSetsResource.class);

    @Autowired
    private DataProviderService providerService;

    @Autowired
    private DataSetService dataSetService;

    @Context
    private UriInfo uriInfo;

    @PathParam(P_PROVIDER)
    private String providerId;


    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    public ResultSlice<DataSet> getDataSets(@QueryParam(F_START_FROM) String startFrom)
            throws ProviderNotExistsException {
        return dataSetService.getDataSets(providerId, startFrom, ParamUtil.numberOfElements());
    }


    @POST
    public Response createDataSet(@FormParam(F_DATASET) String dataSetId, @FormParam(F_DESCRIPTION) String description)
            throws ProviderNotExistsException, DataSetAlreadyExistsException {
        ParamUtil.require(F_DATASET, dataSetId);
        DataSet dataSet = dataSetService.createDataSet(providerId, dataSetId, description);
        EnrichUriUtil.enrich(uriInfo, dataSet);
        return Response.created(dataSet.getUri()).build();
    }
}
