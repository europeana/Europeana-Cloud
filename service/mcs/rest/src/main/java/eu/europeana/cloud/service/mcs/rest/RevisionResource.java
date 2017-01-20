package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.utils.RevisionUtils;
import eu.europeana.cloud.common.utils.Tags;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RevisionIsNotValidException;
import eu.europeana.cloud.service.mcs.exception.RevisionNotExistsException;
import jersey.repackaged.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import javax.ws.rs.Consumes;
import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static eu.europeana.cloud.common.web.ParamConstants.*;

/**
 * Created by Tarek on 8/2/2016.
 */

@Path("/records/{" + P_CLOUDID + "}/representations/{" + P_REPRESENTATIONNAME
        + "}/versions/{" + P_VER + "}/revisions")
@Component
@Scope("request")
public class RevisionResource {
    private static final Logger LOGGER = LoggerFactory.getLogger("RequestsLogger");

    @Autowired
    private RecordService recordService;

    @Autowired
    private DataSetService dataSetService;

    /**
     * Adds a new revision to representation version.If a revision already existed it will update it.
     * <strong>Read permissions required.</strong>
     *
     * @param globalId           cloud id of the record (required).
     * @param schema             schema of representation (required).
     * @param version            a specific version of the representation(required).
     * @param revisionName       the name of revision (required).
     * @param revisionProviderId revision provider id (required).
     * @param tag                tag flag (acceptance,published,deleted)
     * @return URI to specific revision with specific tag inside a version.TODO
     * @throws RepresentationNotExistsException representation does not exist in specified version
     * @throws RevisionIsNotValidException      if the added revision was not valid
     * @statuscode 201 object has been created.
     */
    @POST
    @Path("/{" + P_REVISION_NAME + "}/revisionProvider/{" + P_REVISION_PROVIDER_ID + "}/tag/{" + P_TAG + "}")
    @PreAuthorize("hasPermission(#globalId.concat('/').concat(#schema).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', read)")
    public Response addRevision(@Context UriInfo uriInfo,
                                @PathParam(P_CLOUDID) final String globalId,
                                @PathParam(P_REPRESENTATIONNAME) final String schema,
                                @PathParam(P_VER) final String version,
                                @PathParam(P_REVISION_NAME) String revisionName,
                                @PathParam(P_TAG) String tag,
                                @PathParam(P_REVISION_PROVIDER_ID) String revisionProviderId
    )
            throws RepresentationNotExistsException, RevisionIsNotValidException, ProviderNotExistsException {
        ParamUtil.validate(P_TAG, tag, Arrays.asList(Tags.ACCEPTANCE.getTag(), Tags.PUBLISHED.getTag(), Tags.DELETED.getTag()));
        String revisionKey = RevisionUtils.getRevisionKey(revisionProviderId, revisionName);
        Revision revision = null;
        try {
            revision = recordService.getRevision(globalId, schema, version, revisionKey);
            if (Tags.ACCEPTANCE.getTag().equals(tag))
                revision.setAcceptance(true);
            else if (Tags.PUBLISHED.getTag().equals(tag))
                revision.setPublished(true);
            else revision.setDeleted(true);
            revision.setUpdateTimeStamp(new Date());
        } catch (RevisionNotExistsException e) {
            revision = createNewRevision(revisionName, revisionProviderId, tag);
        }
        addRevision(globalId, schema, version, revision);
        return Response.created(uriInfo.getAbsolutePath()).build();
    }

    private void addRevision(String globalId, String schema, String version, Revision revision) throws RevisionIsNotValidException, ProviderNotExistsException, RepresentationNotExistsException {
        final String revisionKey = RevisionUtils.getRevisionKey(revision.getRevisionProviderId(), revision.getRevisionName());
        createAssignmentToRevisionOnDataSets(globalId, schema, version, revision.getRevisionProviderId(), revisionKey);
        recordService.addRevision(globalId, schema, version, revision);

        //Those may combined together !
        dataSetService.updateProviderDatasetRepresentation(globalId, schema, version, revision);
        dataSetService.updateLatestProviderDatasetRepresentation(globalId, schema, version, revision);
    }

    private void createAssignmentToRevisionOnDataSets(String globalId, String schema,
                                                      String version, String revisionProviderId, String revisionKey)
            throws ProviderNotExistsException {
        Set<String> dataSets = dataSetService.getDataSets(revisionProviderId, globalId, schema, version);
        for (String dataSet : dataSets) {
            dataSetService.addDataSetsRevisions(revisionProviderId, dataSet, revisionKey, schema, globalId);
        }
    }

    /**
     * Adds a new revision to representation version.If a revision already existed it will override it .
     * <strong>Read permissions required.</strong>
     *
     * @param revision Revision (required).
     * @return URI to revisions inside a version. TODO
     * @throws RepresentationNotExistsException representation does not exist in specified version
     * @statuscode 201 object has been created.
     */
    @POST
    @PreAuthorize("hasPermission(#globalId.concat('/').concat(#schema).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', read)")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response addRevision(@Context UriInfo uriInfo,
                                @PathParam(P_CLOUDID) final String globalId,
                                @PathParam(P_REPRESENTATIONNAME) final String schema,
                                @PathParam(P_VER) final String version,
                                Revision revision
    )
            throws RevisionIsNotValidException, ProviderNotExistsException, RepresentationNotExistsException {

        addRevision(globalId, schema, version, revision);
        return Response.created(uriInfo.getAbsolutePath()).build();
    }


    /**
     * Adds a new revision to representation version.If a revision already existed it will update it.
     * <strong>Read permissions required.</strong>
     *
     * @param globalId           cloud id of the record (required).
     * @param schema             schema of representation (required).
     * @param version            a specific version of the representation(required).
     * @param revisionName       the name of revision (required).
     * @param revisionProviderId revision provider id (required).
     * @param tags               set of tags (acceptance,published,deleted)
     * @return URI to a revision tags inside a version.TODO
     * @throws RepresentationNotExistsException representation does not exist in specified version
     * @throws RevisionIsNotValidException      if the added revision was not valid
     * @statuscode 201 object has been created.
     */

    @POST
    @Path("/{" + P_REVISION_NAME + "}/revisionProvider/{" + P_REVISION_PROVIDER_ID + "}/tags")
    @PreAuthorize("hasPermission(#globalId.concat('/').concat(#schema).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', read)")
    public Response addRevision(@Context UriInfo uriInfo,
                                @PathParam(P_CLOUDID) final String globalId,
                                @PathParam(P_REPRESENTATIONNAME) final String schema,
                                @PathParam(P_VER) final String version,
                                @PathParam(P_REVISION_NAME) String revisionName,
                                @PathParam(P_REVISION_PROVIDER_ID) String revisionProviderId,
                                @FormParam(F_TAGS) Set<String> tags
    )
            throws RepresentationNotExistsException, RevisionIsNotValidException, ProviderNotExistsException {

        ParamUtil.validateTags(tags, new HashSet<>(Sets.newHashSet(Tags.ACCEPTANCE.getTag(), Tags.PUBLISHED.getTag(), Tags.DELETED.getTag())));
        String revisionKey = RevisionUtils.getRevisionKey(revisionProviderId, revisionName);
        Revision revision = null;
        try {
            revision = recordService.getRevision(globalId, schema, version, revisionKey);
            revision.setUpdateTimeStamp(new Date());
        } catch (RevisionNotExistsException e) {
            revision = new Revision(revisionName, revisionProviderId);
        }
        setRevisionTags(revision, tags);
        addRevision(globalId, schema, version, revision);
        return Response.created(uriInfo.getAbsolutePath()).build();
    }


    private Revision createNewRevision(String revisionName, String revisionProviderId, String tag) {
        boolean acceptance = false;
        boolean published = false;
        boolean deleted = false;
        if (Tags.ACCEPTANCE.getTag().equals(tag))
            acceptance = true;
        else if (Tags.PUBLISHED.getTag().equals(tag))
            published = true;
        else deleted = true;
        Revision revision = new Revision(revisionName, revisionProviderId, new Date(), acceptance, published, deleted);
        return revision;
    }


    private Revision setRevisionTags(Revision revision, Set<String> tags) {
        if (tags == null || tags.isEmpty())
            return revision;
        if (tags.contains(Tags.ACCEPTANCE.getTag()))
            revision.setAcceptance(true);
        if (tags.contains(Tags.PUBLISHED.getTag()))
            revision.setPublished(true);
        if (tags.contains(Tags.DELETED.getTag()))
            revision.setDeleted(true);
        return revision;
    }

}
