package eu.europeana.cloud.service.mcs.rest;

import com.qmino.miredot.annotations.ReturnType;
import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.response.CloudVersionRevisionResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.utils.RevisionUtils;
import eu.europeana.cloud.common.utils.Tags;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.stereotype.Component;

import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import static eu.europeana.cloud.common.web.ParamConstants.*;

/**
 * Resource to manage data sets.
 */
@Path("/data-providers/{" + P_PROVIDER + "}/data-sets/{" + P_DATASET + "}")
@Component
@Scope("request")
public class DataSetResource {

    @Autowired
    private DataSetService dataSetService;

    @Autowired
    private MutableAclService mutableAclService;

    @Value("${numberOfElementsOnPage}")
    private int numberOfElementsOnPage;

    private final String DATASET_CLASS_NAME = DataSet.class.getName();

    /**
     * Deletes data set.
     * <strong>Delete permissions required.</strong>
     *
     * @param providerId identifier of the dataset's provider(required).
     * @param dataSetId  identifier of the deleted data set(required).
     * @throws DataSetNotExistsException data set not exists.
     */
    @DELETE
    @PreAuthorize("hasPermission(#dataSetId.concat('/').concat(#providerId), 'eu.europeana.cloud.common.model.DataSet', delete)")
    public void deleteDataSet(@PathParam(P_DATASET) String dataSetId, @PathParam(P_PROVIDER) String providerId)
            throws DataSetNotExistsException {

        dataSetService.deleteDataSet(providerId, dataSetId);

        // let's delete the permissions as well
        String ownersName = SpringUserUtils.getUsername();
        if (ownersName != null) {
            ObjectIdentity dataSetIdentity = new ObjectIdentityImpl(DATASET_CLASS_NAME,
                    dataSetId + "/" + providerId);
            mutableAclService.deleteAcl(dataSetIdentity, false);
        }
    }

    /**
     * Lists representation versions from data set. Result is returned in
     * slices.
     *
     * @param providerId identifier of the dataset's provider (required).
     * @param dataSetId  identifier of a data set (required).
     * @param startFrom  reference to next slice of result. If not provided,
     *                   first slice of result will be returned.
     * @return slice of representation version list.
     * @throws DataSetNotExistsException no such data set exists.
     * @summary get representation versions from a data set
     */
    @GET
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @ReturnType("eu.europeana.cloud.common.response.ResultSlice<eu.europeana.cloud.common.model.Representation>")
    public ResultSlice<Representation> getDataSetContents(@PathParam(P_DATASET) String dataSetId,
                                                          @PathParam(P_PROVIDER) String providerId,
                                                          @QueryParam(F_START_FROM) String startFrom)
            throws DataSetNotExistsException {
        return dataSetService.listDataSet(providerId, dataSetId, startFrom, numberOfElementsOnPage);
    }

    /**
     * Updates description of a data set.
     * <p>
     * <strong>Write permissions required.</strong>
     *
     * @param providerId  identifier of the dataset's provider (required).
     * @param dataSetId   identifier of a data set (required).
     * @param description description of data set
     * @throws DataSetNotExistsException                 no such data set exists.
     * @throws AccessDeniedOrObjectDoesNotExistException there is an attempt to access a resource without the proper permissions.
     *                                                   or the resource does not exist at all
     * @statuscode 204 object has been updated.
     */
    @PUT
    @PreAuthorize("hasPermission(#dataSetId.concat('/').concat(#providerId), 'eu.europeana.cloud.common.model.DataSet', write)")
    public void updateDataSet(@PathParam(P_DATASET) String dataSetId,
                              @PathParam(P_PROVIDER) String providerId,
                              @FormParam(F_DESCRIPTION) String description)
            throws AccessDeniedOrObjectDoesNotExistException, DataSetNotExistsException {
        dataSetService.updateDataSet(providerId, dataSetId, description);
    }

    @Path("/representationsNames")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @GET
    public RepresentationNames getRepresentationsNames(
            @PathParam(P_DATASET) String dataSetId,
            @PathParam(P_PROVIDER) String providerId) throws ProviderNotExistsException, DataSetNotExistsException {

        RepresentationNames representationNames = new RepresentationNames();
        representationNames.setNames(dataSetService.getAllDataSetRepresentationsNames(providerId, dataSetId));
        return representationNames;
    }

    @Path("/representations/{" + P_REPRESENTATIONNAME + "}")
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @ReturnType("eu.europeana.cloud.common.response.ResultSlice<CloudVersionRevisionResponse>")
    @GET
    public ResultSlice<CloudVersionRevisionResponse> getDataSetCloudIdsByRepresentation(
            @PathParam(P_DATASET) String dataSetId, @PathParam(P_PROVIDER) String providerId,
            @PathParam(P_REPRESENTATIONNAME) String representationName, @QueryParam(F_DATE_FROM) String dateFrom, @QueryParam(F_TAG) String tag, @QueryParam(F_START_FROM) String startFrom)
            throws ProviderNotExistsException, DataSetNotExistsException {
        Tags tags = Tags.valueOf(tag.toUpperCase());
        DateTime utc = new DateTime(dateFrom, DateTimeZone.UTC);

        if (Tags.PUBLISHED.equals(tags))
            return dataSetService.getDataSetCloudIdsByRepresentationPublished(dataSetId, providerId, representationName, utc.toDate(), startFrom, numberOfElementsOnPage);
        throw new IllegalArgumentException("Only PUBLISHED tag is supported for this request.");
    }


    /**
     * get the latest cloud identifier,revision timestamp that belong to data set of a specified provider for a specific representation and revision and where revision timestamp is bigger than a specified date ;
     *
     * @param dataSetId          data set identifier
     * @param providerId         provider identifier
     * @param revisionName       revision name
     * @param revisionProvider   revision provider
     * @param representationName representation name
     * @param dateFrom           date of latest revision
     * @return Lists all cloud identifiers,timestamps that belong to data set from the specified provider for a specific representation and revision and where revision timestamp is bigger than a specified date ;
     * @throws ProviderNotExistsException
     * @throws DataSetNotExistsException
     */

    @Path("/revision/{" + P_REVISION_NAME + "}/revisionProvider/{" + REVISION_PROVIDER + "}/representations/{" + P_REPRESENTATIONNAME + "}")
    @Produces({MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    @ReturnType("eu.europeana.cloud.common.model.CloudIdAndTimestampResponse")
    @GET
    public CloudIdAndTimestampResponse getDataSetCloudIdsByRepresentationAndRevision(
            @PathParam(P_DATASET) String dataSetId, @PathParam(P_PROVIDER) String providerId,
            @PathParam(P_REVISION_NAME) String revisionName, @PathParam(REVISION_PROVIDER) String revisionProvider, @PathParam(P_REPRESENTATIONNAME) String representationName, @QueryParam(F_DATE_FROM) String dateFrom)
            throws ProviderNotExistsException, DataSetNotExistsException

    {
        DateTime utc = new DateTime(dateFrom, DateTimeZone.UTC);
        String revisionId = RevisionUtils.getRevisionKey(providerId, revisionName);
        CloudIdAndTimestampResponse cloudIdAndTimestampResponse = dataSetService.getLatestDataSetCloudIdByRepresentationAndRevision(dataSetId, providerId, revisionId, representationName, utc.toDate());
        return cloudIdAndTimestampResponse;
    }
}

