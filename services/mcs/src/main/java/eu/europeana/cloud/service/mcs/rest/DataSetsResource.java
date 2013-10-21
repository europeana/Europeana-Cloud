package eu.europeana.cloud.service.mcs.rest;

import static eu.europeana.cloud.service.mcs.rest.PathConstants.*;

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;

import eu.europeana.cloud.service.mcs.service.DataProviderService;
import java.util.List;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.service.mcs.service.DataSetService;

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
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public List<DataSet> getDataSets()
            throws ProviderNotExistsException {
        return dataSetService.getDataSets(providerId);
    }


    @POST
    public Response createDataSet(@FormParam(F_DATASET) String dataSetId)
            throws ProviderNotExistsException, DataSetAlreadyExistsException {
        ParamUtil.require(F_DATASET, dataSetId);
        DataSet dataSet = dataSetService.createDataSet(providerId, dataSetId);
        EnrichUriUtil.enrich(uriInfo, dataSet);
        return Response.created(dataSet.getUri()).build();
    }
}