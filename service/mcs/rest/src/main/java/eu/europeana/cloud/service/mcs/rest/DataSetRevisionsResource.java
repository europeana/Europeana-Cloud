package eu.europeana.cloud.service.mcs.rest;

/**
 * @author akrystian
 */

import com.qmino.miredot.annotations.ReturnType;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.stereotype.Component;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import static eu.europeana.cloud.common.web.ParamConstants.F_START_FROM;
import static eu.europeana.cloud.common.web.ParamConstants.P_DATASET;
import static eu.europeana.cloud.common.web.ParamConstants.P_PROVIDER;
import static eu.europeana.cloud.common.web.ParamConstants.P_REPRESENTATIONNAME;
import static eu.europeana.cloud.common.web.ParamConstants.P_REVISIONID;

/**
 * Resource to manage data sets.
 */
@Path("/data-providers/{" + P_PROVIDER + "}/data-sets/{" + P_DATASET + "}/representationNames/{" + P_REPRESENTATIONNAME + "}/revisons/{" + P_REVISIONID + "}")
@Component
@Scope("request")
public class DataSetRevisionsResource{

    @Autowired
    private DataSetService dataSetService;

    @Value("${numberOfElementsOnPage}")
    private int numberOfElementsOnPage;

    @Autowired
    private MutableAclService mutableAclService;

    /**
     * Lists cloudIds from data set. Result is returned in
     * slices.
     * @summary get representation versions from a data set
     *
     * @param providerId  identifier of the dataset's provider (required).
     * @param dataSetId  identifier of a data set (required).
     * @param startFrom reference to next slice of result. If not provided,
     * first slice of result will be returned.
     * @return slice of representation version list.
     * @throws DataSetNotExistsException no such data set exists.
     */
    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @ReturnType("eu.europeana.cloud.common.response.ResultSlice<String>")
    public ResultSlice<String> getDataSetContents(@PathParam(P_DATASET) String dataSetId,
                                                          @PathParam(P_PROVIDER) String providerId, @PathParam(P_REPRESENTATIONNAME) String representationName,
                                                  @PathParam(P_REVISIONID) String revisionId,
                                                          @QueryParam(F_START_FROM) String startFrom)
            throws DataSetNotExistsException {
        return dataSetService.getDataSetsRevisions(providerId, dataSetId, revisionId, representationName, startFrom, numberOfElementsOnPage);
    }

}
