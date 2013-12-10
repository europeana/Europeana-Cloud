package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.F_DESCRIPTION;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.F_START_FROM;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.P_DATASET;
import static eu.europeana.cloud.service.mcs.rest.ParamConstants.P_PROVIDER;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Resource to manage data sets.
 */
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

    @Value("${numberOfElementsOnPage}")
    private int numberOfElementsOnPage;


    /**
     * Deletes data set.
     * 
     * @throws ProviderNotExistsException
     *             provider not exists
     * @throws DataSetNotExistsException
     *             data set not exists.
     */
    @DELETE
    public void deleteDataSet()
            throws ProviderNotExistsException, DataSetNotExistsException {
        dataSetService.deleteDataSet(providerId, dataSetId);
    }


    /**
     * Lists representation versions from data set. Result is returned in slices.
     * 
     * @param startFrom
     *            reference to next slice of result. If not provided, first slice of result will be returned.
     * @return slice of representation version list.
     * @throws DataSetNotExistsException
     *             no such data set exists.
     */
    @GET
    @Produces({ MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON })
    public ResultSlice<Representation> getDataSetContents(@QueryParam(F_START_FROM) String startFrom)
            throws DataSetNotExistsException {
        return dataSetService.listDataSet(providerId, dataSetId, startFrom, numberOfElementsOnPage);
    }


    /**
     * Updates description of data set. *
     * 
     * @param description
     *            description of data set
     * @throws DataSetNotExistsException
     *             no such data set exists.
     * @statuscode 204 object has been updated.
     */
    @PUT
    public void updateDataSet(@FormParam(F_DESCRIPTION) String description)
            throws DataSetNotExistsException {
        dataSetService.updateDataSet(providerId, dataSetId, description);
    }
}
