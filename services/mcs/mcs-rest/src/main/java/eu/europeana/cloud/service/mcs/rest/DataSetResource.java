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
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;

@Path("/data-providers/{" + P_PROVIDER + "}/data-sets/{" + P_DATASET + "}")
@Component
public class DataSetResource {

    private static final Logger log = LoggerFactory.getLogger(DataSetResource.class);

    @Autowired
    private DataSetService dataSetService;

    @PathParam(P_PROVIDER)
    private String providerId;

    @PathParam(P_DATASET)
    private String dataSetId;

    @Context
    private UriInfo uriInfo;


    @DELETE
    public void deleteDataSet()
            throws ProviderNotExistsException, DataSetNotExistsException {
        dataSetService.deleteDataSet(providerId, dataSetId);
    }


    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    public ResultSlice<Representation> getDataSetContents(@QueryParam(F_START_FROM) String startFrom) {
        return dataSetService.listDataSet(providerId, dataSetId, startFrom, ParamUtil.numberOfElements());
    }


    @PUT
    public Response createDataSet(@FormParam(F_DESCRIPTION) String description) {
        DataSet dataSet = dataSetService.createDataSet(providerId, dataSetId, description);
        EnrichUriUtil.enrich(uriInfo, dataSet);
        return Response.created(dataSet.getUri()).build();
    }
}
