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

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

import static eu.europeana.cloud.common.web.ParamConstants.*;

/**
 * Resource to manage data sets.
 */
@Path("/data-providers/{" + P_PROVIDER + "}/data-sets/{" + P_DATASET + "}/representationNames/{" + P_REPRESENTATIONNAME + "}/revisons/{" + P_REVISIONID + "}")
@Component
@Scope("request")
public class DataSetRevisionsResource {

    @Autowired
    private DataSetService dataSetService;

    @Value("${numberOfElementsOnPage}")
    private int numberOfElementsOnPage;

    @Autowired
    private MutableAclService mutableAclService;

    /**
     * Lists cloudIds from data set. Result is returned in
     * slices.
     *
     * @param providerId   identifier of the dataset's provider (required).
     * @param dataSetId    identifier of a data set (required).
     * @param startCloudId reference to next slice of result. If not provided,
     *                     first slice of result will be returned.
     * @return slice of representation version list.
     * @throws DataSetNotExistsException no such data set exists.
     * @summary get representation versions from a data set
     */
    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @ReturnType("eu.europeana.cloud.common.response.ResultSlice<String>")
    public Response getDataSetContents(@PathParam(P_PROVIDER) String providerId,
                                                  @PathParam(P_DATASET) String dataSetId,
                                                  @PathParam(P_REPRESENTATIONNAME) String representationName,
                                                  @PathParam(P_REVISIONID) String revisionId,
                                                  @QueryParam(F_START_FROM) String startCloudId,
                                                  @QueryParam(F_LIMIT) @Min(1) @Max(10000) int limitParm)
            throws DataSetNotExistsException {
        final int limitWithNextSlice = (limitParm >= 1) ? limitParm + 1 : numberOfElementsOnPage + 1;
        ResultSlice<String> slice = new ResultSlice<>();
        final List<String> cloudIds = dataSetService.getDataSetsRevisions(providerId, dataSetId, revisionId, representationName, startCloudId, limitWithNextSlice);
        if (cloudIds.size() == limitWithNextSlice) {
            setNextSliceAndRemoveLastElement(slice, limitWithNextSlice, cloudIds);
        }
        slice.setResults(cloudIds);
        return Response.ok(slice).build();
    }

    private void setNextSliceAndRemoveLastElement(ResultSlice<String> slice, int limitWithNextSlice, List<String> cloudIds) {
        String nextSlice = cloudIds.remove(limitWithNextSlice - 1);
        slice.setNextSlice(nextSlice);
    }

}
