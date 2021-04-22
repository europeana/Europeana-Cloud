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
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.*;

/**
 * Created by Tarek on 8/2/2016.
 */
@RestController
public class RevisionResource {
    private static final Logger LOGGER = LoggerFactory.getLogger(RevisionResource.class.getName());

    private final RecordService recordService;
    private final DataSetService dataSetService;

    public RevisionResource(RecordService recordService, DataSetService dataSetService) {
        this.recordService = recordService;
        this.dataSetService = dataSetService;
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
     * @param tag                tag flag (acceptance,published,deleted)
     * @return URI to specific revision with specific tag inside a version.
     * @throws RepresentationNotExistsException representation does not exist in specified version
     * @throws RevisionIsNotValidException      if the added revision was not valid
     * @statuscode 201 object has been created.
     */
    @PostMapping(value = REVISION_ADD_WITH_PROVIDER_TAG)
    @PreAuthorize("hasPermission(#cloudId.concat('/').concat(#representationName).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', read)")
    public ResponseEntity<String> addRevision(
            HttpServletRequest httpServletRequest,
            @PathVariable final String cloudId,
            @PathVariable final String representationName,
            @PathVariable final String version,
            @PathVariable String revisionName,
            @PathVariable String revisionProviderId,
            @PathVariable String tag) throws RepresentationNotExistsException, RevisionIsNotValidException {

        ParamUtil.validate("tag", tag,
                Arrays.asList(Tags.ACCEPTANCE.getTag(), Tags.PUBLISHED.getTag(), Tags.DELETED.getTag()));

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
     * @return URI to revisions inside a version.
     * @throws RepresentationNotExistsException representation does not exist in specified version
     * @statuscode 201 object has been created.
     */
    @PostMapping(value = REVISION_ADD, consumes = {MediaType.APPLICATION_JSON_VALUE})
    @PreAuthorize("hasPermission(#cloudId.concat('/').concat(#representationName).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', read)")
    public ResponseEntity<?> addRevision(
            HttpServletRequest httpServletRequest,
            @PathVariable final String cloudId,
            @PathVariable final String representationName,
            @PathVariable final String version,
            @RequestBody Revision revision) throws RevisionIsNotValidException, RepresentationNotExistsException {

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
     * @return URI to a revision tags inside a version.
     * @throws RepresentationNotExistsException representation does not exist in specified version
     * @throws RevisionIsNotValidException      if the added revision was not valid
     * @statuscode 201 object has been created.
     */
    @PostMapping(value = REVISION_ADD_WITH_PROVIDER)
    @PreAuthorize("hasPermission(#cloudId.concat('/').concat(#representationName).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', read)")
    public ResponseEntity<Revision> addRevision(
            HttpServletRequest httpServletRequest,
            @PathVariable final String cloudId,
            @PathVariable final String representationName,
            @PathVariable final String version,
            @PathVariable String revisionName,
            @PathVariable String revisionProviderId,
            @RequestParam(defaultValue = "") Set<String> tags ) throws RepresentationNotExistsException,
                                                        RevisionIsNotValidException {

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
    @DeleteMapping(value = REVISION_DELETE)
    @PreAuthorize("hasPermission(#cloudId.concat('/').concat(#representationName).concat('/').concat(#version),"
            + " 'eu.europeana.cloud.common.model.Representation', read)")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteRevision(
            @PathVariable String cloudId,
            @PathVariable String representationName,
            @PathVariable String version,
            @PathVariable String revisionName,
            @PathVariable String revisionProviderId,
            @RequestParam String revisionTimestamp ) throws RepresentationNotExistsException {

        DateTime timestamp = new DateTime(revisionTimestamp, DateTimeZone.UTC);
        dataSetService.deleteRevision(cloudId, representationName, version, revisionName, revisionProviderId, timestamp.toDate());
    }

    private void addRevision(String globalId, String schema, String version, Revision revision)
            throws RevisionIsNotValidException, RepresentationNotExistsException {

        recordService.addRevision(globalId, schema, version, revision);
        // insert information in extra table
        recordService.insertRepresentationRevision(globalId, schema, revision.getRevisionProviderId(),
                revision.getRevisionName(), version, revision.getCreationTimeStamp());

        dataSetService.updateAllRevisionDatasetsEntries(globalId, schema, version, revision);
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
            LOGGER.warn("Invalid URI: '{}'", httpServletRequest.getRequestURL(), urise);
        }

        ResponseEntity.BodyBuilder bb = ResponseEntity.created(resultURI);
        if(entity != null) {
            return bb.body(entity);
        } else {
            return bb.build();
        }
    }
}
