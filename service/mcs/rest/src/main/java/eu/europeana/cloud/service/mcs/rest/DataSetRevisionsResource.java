package eu.europeana.cloud.service.mcs.rest;

/**
 * @author akrystian
 */

import com.qmino.miredot.annotations.ReturnType;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.stereotype.Component;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static eu.europeana.cloud.common.web.ParamConstants.*;

/**
 * Resource to manage data sets.
 */
@Path("/data-providers/{" + P_PROVIDER + "}/data-sets/{" + P_DATASET + "}/representations/{" +
        P_REPRESENTATIONNAME + "}/revisions/{" + P_REVISION_NAME + "}/revisionProvider/{" + P_REVISION_PROVIDER_ID + "}")

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
     * @param providerId         identifier of the dataset's provider.
     * @param dataSetId          identifier of a data set.
     * @param representationName representation name.
     * @param revisionName       name of the revision
     * @param revisionProviderId provider of revision
     * @param revisionTimestamp  timestamp used for identifying revision, must be in UTC format
     * @param startFrom          reference to next slice of result. If not provided,
     *                           first slice of result will be returned.
     * @return slice of cloud id with tags of the revision list.
     * @throws DataSetNotExistsException no such data set exists.
     * @summary get representation versions from a data set
     */
    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @ReturnType("eu.europeana.cloud.common.response.ResultSlice<CloudTagsResponse>")
    public Response getDataSetContents(@PathParam(P_PROVIDER) String providerId,
                                       @PathParam(P_DATASET) String dataSetId,
                                       @PathParam(P_REPRESENTATIONNAME) String representationName,
                                       @PathParam(P_REVISION_NAME) String revisionName,
                                       @PathParam(P_REVISION_PROVIDER_ID) String revisionProviderId,
                                       @QueryParam(F_REVISION_TIMESTAMP) String revisionTimestamp,
                                       @QueryParam(F_START_FROM) String startFrom,
                                       @QueryParam(F_LIMIT) int limitParam)
            throws DataSetNotExistsException, ProviderNotExistsException {
        // when limitParam is specified we can retrieve more results than configured number of elements per page
        final int limitWithNextSlice = (limitParam > 0 && limitParam <= 10000) ? limitParam : numberOfElementsOnPage;
        // validate parameters
        if (revisionTimestamp == null) {
            throw new WebApplicationException("Revision timestamp parameter cannot be null");
        }
        DateTime timestamp = new DateTime(revisionTimestamp, DateTimeZone.UTC);

        ResultSlice<CloudTagsResponse> result = dataSetService.getDataSetsRevisions(providerId, dataSetId, revisionProviderId, revisionName, timestamp.toDate(), representationName, startFrom, limitWithNextSlice);
        return Response.ok(result).build();
    }


    /**
     * Remove specific revision from a specific dataSet
     *
     * @param dataSetId          data set identifier
     * @param providerId         provider identifier
     * @param revisionName       revision name
     * @param revisionProviderId revision provider
     * @param representationName representation name
     * @param revisionTimestamp  revision timestamp
     * @throws ProviderNotExistsException
     * @throws DataSetNotExistsException
     */

    @Path("/records/{" + P_CLOUDID + "}/versions/{" + P_VER + "}")
    @DELETE
    public void deleteRevisionFromDataSet(
            @PathParam(P_DATASET) String dataSetId,
            @PathParam(P_PROVIDER) String providerId,
            @PathParam(P_REPRESENTATIONNAME) String representationName,
            @PathParam(P_REVISION_NAME) String revisionName,
            @PathParam(P_REVISION_PROVIDER_ID) String revisionProviderId,
            @PathParam(P_CLOUDID) String cloudId,
            @PathParam(P_VER) String version,
            @QueryParam(F_REVISION_TIMESTAMP) String revisionTimestamp

    )
            throws ProviderNotExistsException, DataSetNotExistsException, RepresentationNotExistsException

    {
        if (revisionTimestamp == null) {
            throw new WebApplicationException("Revision timestamp parameter cannot be null");
        }
        DateTime timestamp = new DateTime(revisionTimestamp, DateTimeZone.UTC);
        dataSetService.deleteRevisionFromDataSet(dataSetId, providerId, revisionName, revisionProviderId, timestamp.toDate(), representationName, version, cloudId);
    }
}


