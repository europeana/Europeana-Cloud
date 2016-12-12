package eu.europeana.cloud.service.mcs.rest;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.qmino.miredot.annotations.ReturnType;
import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.response.CloudVersionRevisionResponse;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.utils.Tags;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
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

import javax.ws.rs.*;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.annotation.XmlRootElement;

import java.util.*;

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
     *
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
    @ReturnType("eu.europeana.cloud.common.response.ResultSlice<eu.europeana.cloud.common.model.Representation>")
    public ResultSlice<Representation> getDataSetContents(@PathParam(P_DATASET) String dataSetId,
    		@PathParam(P_PROVIDER) String providerId,
    		@QueryParam(F_START_FROM) String startFrom)
            throws DataSetNotExistsException {
        return dataSetService.listDataSet(providerId, dataSetId, startFrom, numberOfElementsOnPage);
    }

    /**
     * Updates description of a data set.
     *
     * <strong>Write permissions required.</strong>
     *
     * @param providerId  identifier of the dataset's provider (required).
     * @param dataSetId  identifier of a data set (required).
     * @param description description of data set
     * @throws DataSetNotExistsException no such data set exists.
     * @throws AccessDeniedOrObjectDoesNotExistException there is an attempt to access a resource without the proper permissions.
     * or the resource does not exist at all
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
            @PathParam(P_REPRESENTATIONNAME) String representationName, @QueryParam(F_DATE_FROM) String dateFrom, @QueryParam(P_TAG) String tag, @QueryParam(F_START_FROM) String startFrom)
            throws ProviderNotExistsException, DataSetNotExistsException {
        Tags tags = Tags.valueOf(tag.toUpperCase());
        DateTime utc = new DateTime(dateFrom, DateTimeZone.UTC);

        if (Tags.PUBLISHED.equals(tags))
            return dataSetService.getDataSetCloudIdsByRepresentationPublished(dataSetId, providerId, representationName, utc.toDate(), startFrom, numberOfElementsOnPage);
        throw new IllegalArgumentException("Only PUBLISHED tag is supported for this request.");
    }

    /**
     *
     * Gives the versionId of specified representation that has the newest revision (by revision timestamp) with given name.
     *
     * @param dataSetId dataset identifier
     * @param providerId dataset owner
     * @param cloudId   representation cloud identifier
     * @param representationName representation name
     * @param revisionName revision name
     * @param revisionProviderId revision owner
     * @return version identifier of representation
     * @throws DataSetNotExistsException
     */
    @Path("/latelyRevisionedVersion")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @GET
    public Response getLatelyTaggedRecords(
            @PathParam(P_DATASET) String dataSetId,
            @PathParam(P_PROVIDER) String providerId,
            @QueryParam(P_CLOUDID) String cloudId,
            @QueryParam(P_REPRESENTATIONNAME) String representationName,
            @QueryParam(P_REVISION_NAME) String revisionName,
            @QueryParam(P_REVISION_PROVIDER_ID) String revisionProviderId,
            @QueryParam("withTag") String taggedBy,
            @QueryParam("withoutTag") String notTaggedBy
            ) throws DataSetNotExistsException {

        ParamUtil.require(P_REPRESENTATIONNAME, representationName);
        ParamUtil.require(P_REVISION_NAME, revisionName);
        ParamUtil.require(P_REVISION_PROVIDER_ID, revisionProviderId);
        validateTagParams(taggedBy, notTaggedBy);
        Tags tag = createTagFromParameters(taggedBy, notTaggedBy);

        if (isCloudIdProvided(cloudId)) {
            String versionId = dataSetService.getLatestVersionForGivenRevision(dataSetId, providerId, cloudId, representationName, revisionName, revisionProviderId);
            if (versionId != null) {
                return Response.ok().entity(versionId).build();
            } else {
                return Response.noContent().build();
            }
        } else {
            DataSetRepresentationsForLatestRevision revisionedRepresentations = dataSetService.getLatestRepresentationsForGivenRevision(dataSetId, providerId, representationName, revisionName, revisionProviderId, tag);
            GenericEntity<List<CloudIdWithVersionId>> entity = new GenericEntity<List<CloudIdWithVersionId>>(buildResponse(revisionedRepresentations)) {};

            return Response.ok().entity(entity).build();
        }
    }

    private boolean isCloudIdProvided(String cloudId) {
        return cloudId != null;
    }

    private void validateTagParams(String taggedBy, String notTaggedBy) {
        if (taggedBy != null && notTaggedBy != null) {
            ErrorInfo errorInfo = new ErrorInfo(McsErrorCode.OTHER.name(), "Only one tag parameter can be non empty");
            throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST).entity(errorInfo).build());
        }
    }

    private Tags createTagFromParameters(String taggedBy, String notTaggedBy){
        if(taggedBy != null){
            Tags tag = Tags.valueOf(taggedBy);
            tag.setValue(true);
            return tag;
        }
        if(notTaggedBy != null){
            Tags tag = Tags.valueOf(notTaggedBy);
            tag.setValue(false);
            return tag;
        }
        return null;
    }

    private List<CloudIdWithVersionId> buildResponse(DataSetRepresentationsForLatestRevision representations){
        List<CloudIdWithVersionId> entity = new ArrayList<>();
        if(representations == null)
            return entity;
        for(Representation rep : representations.getRepresentations()){
            CloudIdWithVersionId CloudIdWithVersionId = new CloudIdWithVersionId();
            CloudIdWithVersionId.setCloudId(rep.getCloudId());
            CloudIdWithVersionId.setVersionId(rep.getVersion());
            entity.add(CloudIdWithVersionId);
        }
        return entity;

    }
}

@XmlRootElement
class CloudIdWithVersionId {
    private String cloudId;
    private String versionId;

    public String getCloudId() {
        return cloudId;
    }

    public void setCloudId(String cloudId) {
        this.cloudId = cloudId;
    }

    public String getVersionId() {
        return versionId;
    }

    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }
}
