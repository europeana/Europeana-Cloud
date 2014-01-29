package eu.europeana.cloud.service.mcs.rest;

import static eu.europeana.cloud.common.web.ParamConstants.F_DESCRIPTION;
import static eu.europeana.cloud.common.web.ParamConstants.F_START_FROM;
import static eu.europeana.cloud.common.web.ParamConstants.P_DATASET;
import static eu.europeana.cloud.common.web.ParamConstants.P_PROVIDER;

import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;

/**
 * Resource to manage data sets.
 */
@Path("/data-providers/{" + P_PROVIDER + "}/data-sets/{" + P_DATASET + "}")
@Component
@Scope("request")
public class DataSetResource {

    @Autowired
    private DataSetService dataSetService;

    @PathParam(P_PROVIDER)
    private String providerId;

    @PathParam(P_DATASET)
    private String dataSetId;

    @Value("${numberOfElementsOnPage}")
    private int numberOfElementsOnPage;


    /**
     * Deletes data set.
     * 
     * @throws DataSetNotExistsException
     *             data set not exists.
     */
    @DELETE
    public void deleteDataSet()
            throws DataSetNotExistsException {
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
     * Updates description of data set.
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
