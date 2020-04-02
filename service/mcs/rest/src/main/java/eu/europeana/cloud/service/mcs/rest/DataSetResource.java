package eu.europeana.cloud.service.mcs.rest;

import com.qmino.miredot.annotations.ReturnType;
import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.RepresentationNames;
import eu.europeana.cloud.common.response.CloudVersionRevisionResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.utils.Tags;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import eu.europeana.cloud.service.mcs.utils.ParamUtil;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.web.bind.annotation.*;

import javax.ws.rs.FormParam;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import static eu.europeana.cloud.common.web.ParamConstants.*;

/**
 * Resource to manage data sets.
 */
@RestController
@RequestMapping("/data-providers/{" + P_PROVIDER + "}/data-sets/{" + P_DATASET + "}")
@Scope("request")
public class DataSetResource {
    private final String DATASET_CLASS_NAME = DataSet.class.getName();

    @Autowired
    private DataSetService dataSetService;

    @Autowired
    private MutableAclService mutableAclService;

    @Value("${numberOfElementsOnPage}")
    private int numberOfElementsOnPage;

    /**
     * Deletes data set.
     * <strong>Delete permissions required.</strong>
     *
     * @param providerId identifier of the dataset's provider(required).
     * @param dataSetId  identifier of the deleted data set(required).
     * @throws DataSetNotExistsException data set not exists.
     */
    @DeleteMapping
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
    @GetMapping(produces = {MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @ReturnType("eu.europeana.cloud.common.response.ResultSlice<eu.europeana.cloud.common.model.Representation>")
    public ResultSlice<Representation> getDataSetContents(@Context UriInfo uriInfo,
                                                          @PathParam(P_DATASET) String dataSetId,
                                                          @PathParam(P_PROVIDER) String providerId,
                                                          @QueryParam(F_START_FROM) String startFrom)
            throws DataSetNotExistsException {
        ResultSlice<Representation> representations = dataSetService.listDataSet(providerId, dataSetId, startFrom, numberOfElementsOnPage);
        for (Representation rep : representations.getResults()) {
            EnrichUriUtil.enrich(uriInfo, rep);
        }
        return representations;
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
    @PutMapping
    @PreAuthorize("hasPermission(#dataSetId.concat('/').concat(#providerId), 'eu.europeana.cloud.common.model.DataSet', write)")
    public void updateDataSet(@PathParam(P_DATASET) String dataSetId,
                              @PathParam(P_PROVIDER) String providerId,
                              @FormParam(F_DESCRIPTION) String description)
            throws AccessDeniedOrObjectDoesNotExistException, DataSetNotExistsException {
        dataSetService.updateDataSet(providerId, dataSetId, description);
    }

    @GetMapping(value = "/representationsNames", produces = {MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public RepresentationNames getRepresentationsNames(
            @PathParam(P_DATASET) String dataSetId,
            @PathParam(P_PROVIDER) String providerId) throws ProviderNotExistsException, DataSetNotExistsException {

        RepresentationNames representationNames = new RepresentationNames();
        representationNames.setNames(dataSetService.getAllDataSetRepresentationsNames(providerId, dataSetId));
        return representationNames;
    }

    @ReturnType("eu.europeana.cloud.common.response.ResultSlice<CloudVersionRevisionResponse>")
    @GetMapping(value = "/representations/{" + P_REPRESENTATIONNAME + "}", produces = {MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public ResultSlice<CloudVersionRevisionResponse> getDataSetCloudIdsByRepresentation(
            @PathParam(P_DATASET) String dataSetId, @PathParam(P_PROVIDER) String providerId,
            @PathParam(P_REPRESENTATIONNAME) String representationName, @QueryParam(F_DATE_FROM) String dateFrom, @QueryParam(F_TAG) String tag, @QueryParam(F_START_FROM) String startFrom)
            throws ProviderNotExistsException, DataSetNotExistsException {
        Tags tags = Tags.valueOf(tag.toUpperCase());
        DateTime utc = new DateTime(dateFrom, DateTimeZone.UTC);

        if (Tags.PUBLISHED.equals(tags)) {
            return dataSetService.getDataSetCloudIdsByRepresentationPublished(dataSetId, providerId, representationName, utc.toDate(), startFrom, numberOfElementsOnPage);
        }
        throw new IllegalArgumentException("Only PUBLISHED tag is supported for this request.");
    }

    /**
     * get a list of the latest cloud identifiers,revision timestamps that belong to data set of a specified provider for a specific representation and revision.
     * This list will contain one row per revision per cloudId;
     *
     * @param dataSetId          data set identifier
     * @param providerId         provider identifier
     * @param revisionName       revision name
     * @param revisionProvider   revision provider
     * @param representationName representation name
     * @param startFrom          cloudId to start from
     * @param isDeleted          revision marked-deleted
     * @return slice of the latest cloud identifier,revision timestamp that belong to data set of a specified provider for a specific representation and revision
     * This list will contain one row per revision per cloudId ;
     * @throws ProviderNotExistsException
     * @throws DataSetNotExistsException
     */
    @ReturnType("eu.europeana.cloud.common.response.ResultSlice<CloudIdAndTimestampResponse>")
    @GetMapping(value = "/revision/{" + P_REVISION_NAME + "}/revisionProvider/{" + REVISION_PROVIDER + "}/representations/{" + P_REPRESENTATIONNAME + "}", produces = {MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON})
    public ResultSlice<CloudIdAndTimestampResponse> getDataSetCloudIdsByRepresentationAndRevision(
            @PathParam(P_DATASET) String dataSetId, @PathParam(P_PROVIDER) String providerId,
            @PathParam(P_REVISION_NAME) String revisionName, @PathParam(REVISION_PROVIDER) String revisionProvider, @PathParam(P_REPRESENTATIONNAME) String representationName, @QueryParam(F_START_FROM) String startFrom, @QueryParam(IS_DELETED) Boolean isDeleted)
            throws ProviderNotExistsException, DataSetNotExistsException

    {
        ResultSlice<CloudIdAndTimestampResponse> cloudIdAndTimestampResponses = dataSetService.getLatestDataSetCloudIdByRepresentationAndRevision(dataSetId, providerId, revisionName, revisionProvider, representationName, startFrom, isDeleted, numberOfElementsOnPage);
        return cloudIdAndTimestampResponses;
    }

    /**
     * Gives the versionId of specified representation that has the newest revision (by revision timestamp) with given name.
     *
     * @param dataSetId          dataset identifier
     * @param providerId         dataset owner
     * @param cloudId            representation cloud identifier
     * @param representationName representation name
     * @param revisionName       revision name
     * @param revisionProviderId revision owner
     * @return version identifier of representation
     * @throws DataSetNotExistsException
     */
    @GetMapping(value = "/latelyRevisionedVersion", produces = {MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Response getLatelyTaggedRecords(
            @PathParam(P_DATASET) String dataSetId,
            @PathParam(P_PROVIDER) String providerId,
            @QueryParam(F_CLOUDID) String cloudId,
            @QueryParam(F_REPRESENTATIONNAME) String representationName,
            @QueryParam(F_REVISION_NAME) String revisionName,
            @QueryParam(F_REVISION_PROVIDER_ID) String revisionProviderId) throws DataSetNotExistsException {

        ParamUtil.require(F_CLOUDID, cloudId);
        ParamUtil.require(F_REPRESENTATIONNAME, representationName);
        ParamUtil.require(F_REVISION_NAME, revisionName);
        ParamUtil.require(F_REVISION_PROVIDER_ID, revisionProviderId);


        String versionId = dataSetService.getLatestVersionForGivenRevision(dataSetId, providerId, cloudId, representationName, revisionName, revisionProviderId);
        if (versionId != null) {
            return Response.ok().entity(versionId).build();
        } else {
            return Response.noContent().build();
        }
    }
}


