package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.utils.Tags;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RevisionIsNotValidException;
import eu.europeana.cloud.service.mcs.utils.ParamUtil;
import jersey.repackaged.com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static eu.europeana.cloud.common.web.ParamConstants.*;

/**
 * Created by Tarek on 8/2/2016.
 */
@RestController
@RequestMapping("/records/{"+CLOUD_ID+"}/representations/{"+REPRESENTATION_NAME+"}/versions/{"+VERSION+"}/revisions")
@Scope("request")
public class RevisionResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(RevisionResource.class.getName());

    @Autowired
    private RecordService recordService;

    @Autowired
    private DataSetService dataSetService;

    /**
     * Adds a new revision to representation version.
     * <strong>Read permissions required.</strong>
     *
     * @param cloudId           cloud id of the record (required).
     * @param representationName             schema of representation (required).
     * @param version            a specific version of the representation(required).
     * @param revisionName       the name of revision (required).
     * @param revisionProviderId revision provider id (required).
     * @param tag                tag flag (acceptance,published,deleted)
     * @return URI to specific revision with specific tag inside a version.TODO
     * @throws RepresentationNotExistsException representation does not exist in specified version
     * @throws RevisionIsNotValidException      if the added revision was not valid
     * @statuscode 201 object has been created.
     */
    @PostMapping(value = "/{"+REVISION_NAME+"}/revisionProvider/{"+REVISION_PROVIDER_ID+"}/tag/{"+TAG+"}")
    @PreAuthorize("hasPermission(#cloudId.concat('/').concat(#representationName).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', read)")
    public ResponseEntity<String> addRevision(
            HttpServletRequest httpServletRequest,
            @PathVariable(CLOUD_ID) final String cloudId,
            @PathVariable(REPRESENTATION_NAME) final String representationName,
            @PathVariable(VERSION) final String version,
            @PathVariable(REVISION_NAME) String revisionName,
            @PathVariable(REVISION_PROVIDER_ID) String revisionProviderId,
            @PathVariable(TAG) String tag) throws RepresentationNotExistsException,
                                                RevisionIsNotValidException, ProviderNotExistsException {

        ParamUtil.validate(TAG, tag, Arrays.asList(Tags.ACCEPTANCE.getTag(), Tags.PUBLISHED.getTag(), Tags.DELETED.getTag()));
        Revision revision = new Revision(revisionName, revisionProviderId);
        setRevisionTags(revision, new HashSet<>(Arrays.asList(tag)));
        addRevision(cloudId, representationName, version, revision);

        // insert information in extra table
        recordService.insertRepresentationRevision(cloudId, representationName, revisionProviderId,
                revisionName, version, revision.getCreationTimeStamp());

        return createResponseEntity(httpServletRequest, null);
    }

    /**
     * Adds a new revision to representation version.
     * <strong>Read permissions required.</strong>
     *
     * @param revision Revision (required).
     * @return URI to revisions inside a version. TODO
     * @throws RepresentationNotExistsException representation does not exist in specified version
     * @statuscode 201 object has been created.
     */
    @PostMapping(consumes = {MediaType.APPLICATION_JSON_VALUE})
    @PreAuthorize("hasPermission(#cloudId.concat('/').concat(#representationName).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', read)")
    public ResponseEntity<?> addRevision(
            HttpServletRequest httpServletRequest,
            @PathVariable(CLOUD_ID) final String cloudId,
            @PathVariable(REPRESENTATION_NAME) final String representationName,
            @PathVariable(VERSION) final String version,
            Revision revision) throws RevisionIsNotValidException, ProviderNotExistsException, RepresentationNotExistsException {

        addRevision(cloudId, representationName, version, revision);

        // insert information in extra table
        recordService.insertRepresentationRevision(cloudId, representationName, revision.getRevisionProviderId(),
                revision.getRevisionName(), version, revision.getCreationTimeStamp());

        return createResponseEntity(httpServletRequest, null);
    }


    /**
     * Adds a new revision to representation version.
     * <strong>Read permissions required.</strong>
     *
     * @param cloudId           cloud id of the record (required).
     * @param representationName             schema of representation (required).
     * @param version            a specific version of the representation(required).
     * @param revisionName       the name of revision (required).
     * @param revisionProviderId revision provider id (required).
     * @param tags               set of tags (acceptance,published,deleted)
     * @return URI to a revision tags inside a version.TODO
     * @throws RepresentationNotExistsException representation does not exist in specified version
     * @throws RevisionIsNotValidException      if the added revision was not valid
     * @statuscode 201 object has been created.
     */
    @PostMapping(value = "/{"+REVISION_NAME+"}/revisionProvider/{"+REVISION_PROVIDER_ID+"}/tags")
    @PreAuthorize("hasPermission(#cloudId.concat('/').concat(#representationName).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', read)")
    public ResponseEntity<Revision> addRevision(
            HttpServletRequest httpServletRequest,
            @PathVariable(CLOUD_ID) final String cloudId,
            @PathVariable(REPRESENTATION_NAME) final String representationName,
            @PathVariable(VERSION) final String version,
            @PathVariable(REVISION_NAME) String revisionName,
            @PathVariable(REVISION_PROVIDER_ID) String revisionProviderId,
            @RequestParam(required = false) Set<String> tags ) throws RepresentationNotExistsException,
                                                        RevisionIsNotValidException, ProviderNotExistsException {

        ParamUtil.validateTags(tags, new HashSet<>(Sets.newHashSet(Tags.ACCEPTANCE.getTag(), Tags.PUBLISHED.getTag(), Tags.DELETED.getTag())));
        Revision revision = new Revision(revisionName, revisionProviderId);
        setRevisionTags(revision, tags);
        addRevision(cloudId, representationName, version, revision);

        // insert information in extra table
        recordService.insertRepresentationRevision(cloudId, representationName, revisionProviderId, revisionName,
                version, revision.getCreationTimeStamp());

        return createResponseEntity(httpServletRequest, revision);
    }

    /**
     * Remove a revision
     *
     * @param cloudId            cloud Id
     * @param representationName representation name
     * @param version            representation version
     * @param revisionName       revision name
     * @param revisionProviderId revision provider
     * @param revisionTimestamp  revision timestamp
     * @throws ProviderNotExistsException
     * @throws RepresentationNotExistsException
     */
    @DeleteMapping(value = "/{revisionName}/revisionProvider/{revisionProviderId}")
    @PreAuthorize("hasPermission(#cloudId.concat('/').concat(#representationName).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', read)")
    public void deleteRevision(
            @PathVariable(CLOUD_ID) String cloudId,
            @PathVariable(REPRESENTATION_NAME) String representationName,
            @PathVariable(VERSION) String version,
            @PathVariable(REVISION_NAME) String revisionName,
            @PathVariable(REVISION_PROVIDER_ID) String revisionProviderId,
            @RequestParam String revisionTimestamp ) throws RepresentationNotExistsException {

        DateTime timestamp = new DateTime(revisionTimestamp, DateTimeZone.UTC);
        dataSetService.deleteRevision(cloudId, representationName, version, revisionName, revisionProviderId, timestamp.toDate());
    }

    private void addRevision(String globalId, String schema, String version, Revision revision) throws RevisionIsNotValidException, ProviderNotExistsException, RepresentationNotExistsException {
        createAssignmentToRevisionOnDataSets(globalId, schema, version, revision);
        recordService.addRevision(globalId, schema, version, revision);
        dataSetService.updateProviderDatasetRepresentation(globalId, schema, version, revision);
        // insert information in extra table
        recordService.insertRepresentationRevision(globalId, schema, revision.getRevisionProviderId(), revision.getRevisionName(), version, revision.getCreationTimeStamp());
        dataSetService.updateAllRevisionDatasetsEntries(globalId, schema, version, revision);
    }

    private void createAssignmentToRevisionOnDataSets(String globalId, String schema,
                                                      String version, Revision revision)
            throws ProviderNotExistsException, RepresentationNotExistsException {
        Map<String, Set<String>> dataSets = dataSetService.getDataSets(globalId, schema, version);
        for (Map.Entry<String, Set<String>> entry : dataSets.entrySet()) {
            for (String dataset : entry.getValue()) {
                dataSetService.addDataSetsRevisions(entry.getKey(), dataset, revision, schema, globalId);
            }
        }
    }

    private Revision setRevisionTags(Revision revision, Set<String> tags) {
        if (tags == null || tags.isEmpty()) {
            return revision;
        }
        if (tags.contains(Tags.ACCEPTANCE.getTag())) {
            revision.setAcceptance(true);
        }
        if (tags.contains(Tags.PUBLISHED.getTag())) {
            revision.setPublished(true);
        }
        if (tags.contains(Tags.DELETED.getTag())) {
            revision.setDeleted(true);
        }
        return revision;
    }

    private <T> ResponseEntity<T> createResponseEntity(HttpServletRequest httpServletRequest, T entity) {
        URI resultURI = null;
        try {
            resultURI = new URI(httpServletRequest.getRequestURL().toString());
        } catch (URISyntaxException urise) {
            LOGGER.warn("Invalid URI: '"+httpServletRequest.getRequestURL()+"'", urise);
        }

        ResponseEntity.BodyBuilder bb = ResponseEntity.created(resultURI);
        if(entity != null) {
            return bb.body(entity);
        } else {
            return bb.build();
        }
    }
}
