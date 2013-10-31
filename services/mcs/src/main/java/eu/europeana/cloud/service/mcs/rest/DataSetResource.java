package eu.europeana.cloud.service.mcs.rest;

import java.util.List;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.*;

import eu.europeana.cloud.service.mcs.DataProviderService;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
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

import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.DataSetService;

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
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public List<Representation> getDataSetContents() {
        return dataSetService.listDataSet(providerId, dataSetId);
    }


    @PUT
    public Response createDataSet() {
        DataSet dataSet = dataSetService.createDataSet(providerId, dataSetId);
        EnrichUriUtil.enrich(uriInfo, dataSet);
        return Response.created(dataSet.getUri()).build();
    }
}