package eu.europeana.cloud.service.mcs.rest;

import com.google.common.collect.Sets;
import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.utils.Tags;
import eu.europeana.cloud.service.mcs.DataSetService;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RevisionIsNotValidException;
import eu.europeana.cloud.service.mcs.utils.ParamUtil;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.security.access.PermissionEvaluator;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.UriComponentsBuilder;

import javax.servlet.http.HttpServletRequest;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
    private final PermissionEvaluator permissionEvaluator;

    public RevisionResource(RecordService recordService, DataSetService dataSetService, PermissionEvaluator permissionEvaluator) {
        this.recordService = recordService;
        this.dataSetService = dataSetService;
        this.permissionEvaluator = permissionEvaluator;
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
    public ResponseEntity<String> addRevision(
            HttpServletRequest httpServletRequest,
            @PathVariable final String cloudId,
            @PathVariable final String representationName,
            @PathVariable final String version,
            @PathVariable String revisionName,
            @PathVariable String revisionProviderId,
            @PathVariable String tag) throws RepresentationNotExistsException, RevisionIsNotValidException, AccessDeniedOrObjectDoesNotExistException {

        ParamUtil.validate("tag", tag,
                Arrays.asList(Tags.ACCEPTANCE.getTag(), Tags.PUBLISHED.getTag(), Tags.DELETED.getTag()));
        //
        Representation representation = buildRepresentationFromRequestParameters(cloudId, representationName, version);
        Revision revision = buildRevisionFromRequestParams(revisionName, revisionProviderId, tag);
        //
        if (isUserAllowedToAddRevisionTo(representation)) {
            addRevisionToRepresentationVersion(revision, representation);
            return createResponseEntity(httpServletRequest, null);
        } else {
            throw new AccessDeniedOrObjectDoesNotExistException();
        }
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
    public ResponseEntity<?> addRevision(
            HttpServletRequest httpServletRequest,
            @PathVariable final String cloudId,
            @PathVariable final String representationName,
            @PathVariable final String version,
            @RequestBody Revision revision) throws RevisionIsNotValidException, RepresentationNotExistsException, AccessDeniedOrObjectDoesNotExistException {

        //
        Representation representation = buildRepresentationFromRequestParameters(cloudId, representationName, version);
        //
        if (isUserAllowedToAddRevisionTo(representation)) {
            addRevisionToRepresentationVersion(revision, representation);
            return createResponseEntity(httpServletRequest, null);
        } else {
            throw new AccessDeniedOrObjectDoesNotExistException();
        }
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
    public ResponseEntity<Revision> addRevision(
            HttpServletRequest httpServletRequest,
            @PathVariable final String cloudId,
            @PathVariable final String representationName,
            @PathVariable final String version,
            @PathVariable String revisionName,
            @PathVariable String revisionProviderId,
            @RequestParam(defaultValue = "") Set<String> tags ) throws RepresentationNotExistsException,
            RevisionIsNotValidException, AccessDeniedOrObjectDoesNotExistException {

        ParamUtil.validateTags(tags, new HashSet<>(Sets.newHashSet(Tags.ACCEPTANCE.getTag(), Tags.PUBLISHED.getTag(), Tags.DELETED.getTag())));
        //
        Representation representation = buildRepresentationFromRequestParameters(cloudId, representationName, version);
        Revision revision = buildRevisionFromRequestParams(revisionName, revisionProviderId, tags);
        //
        if (isUserAllowedToAddRevisionTo(representation)) {
            addRevisionToRepresentationVersion(revision, representation);
            return createResponseEntity(httpServletRequest, revision);
        } else {
            throw new AccessDeniedOrObjectDoesNotExistException();
        }
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
     * @throws RepresentationNotExistsException when the representation version doesn't exist
     * @throws AccessDeniedOrObjectDoesNotExistException when user doesn't have privileges for the dataset
     */
    @DeleteMapping(value = REVISION_DELETE)
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteRevision(
            @PathVariable String cloudId,
            @PathVariable String representationName,
            @PathVariable String version,
            @PathVariable String revisionName,
            @PathVariable String revisionProviderId,
            @RequestParam String revisionTimestamp ) throws RepresentationNotExistsException, AccessDeniedOrObjectDoesNotExistException {

        //
        Representation representation = buildRepresentationFromRequestParameters(cloudId,representationName,version);
        //
        if (isUserAllowedToAddRevisionTo(representation)) {
            DateTime timestamp = new DateTime(revisionTimestamp, DateTimeZone.UTC);
            dataSetService.deleteRevision(cloudId, representationName, version, revisionName, revisionProviderId, timestamp.toDate());
        } else {
            throw new AccessDeniedOrObjectDoesNotExistException();
        }

    }

    private Representation buildRepresentationFromRequestParameters(String cloudId, String representationName, String version){
        Representation representation = new Representation();
        representation.setCloudId(cloudId);
        representation.setRepresentationName(representationName);
        representation.setVersion(version);
        return representation;
    }

    private Revision buildRevisionFromRequestParams(String revisionName, String revisionProviderId, String tag){
        Revision revision = new Revision(revisionName, revisionProviderId);
        setRevisionTags(revision, new HashSet<>(List.of(tag)));
        return revision;
    }

    private Revision buildRevisionFromRequestParams(String revisionName, String revisionProviderId, Set<String> tags){
        Revision revision = new Revision(revisionName, revisionProviderId);
        setRevisionTags(revision, tags);
        return revision;
    }

    private void addRevisionToRepresentationVersion(Revision revision, Representation representation) throws RepresentationNotExistsException, RevisionIsNotValidException {
        addRevision(
                representation.getCloudId(),
                representation.getRepresentationName(),
                representation.getVersion(),
                revision);

        // insert information in extra table
        recordService.insertRepresentationRevision(
                representation.getCloudId(),
                representation.getRepresentationName(),
                revision.getRevisionProviderId(),
                revision.getRevisionName(),
                representation.getVersion(),
                revision.getCreationTimeStamp());
    }

    private boolean isUserAllowedToAddRevisionTo(Representation representation) throws RepresentationNotExistsException {
        List<CompoundDataSetId> representationDataSets = dataSetService.getAllDatasetsForRepresentationVersion(representation);
        if (representationDataSets.size() != 1) {
            LOGGER.error("Should never happen");
        } else {
            SecurityContext ctx = SecurityContextHolder.getContext();
            Authentication authentication = ctx.getAuthentication();
            //
            String targetId = representationDataSets.get(0).getDataSetId() + "/" + representationDataSets.get(0).getDataSetProviderId();
            return permissionEvaluator.hasPermission(authentication, targetId, DataSet.class.getName(), "read");
        }
        return false;
    }

    private void addRevision(String globalId, String schema, String version, Revision revision)
            throws RevisionIsNotValidException, RepresentationNotExistsException {

        recordService.addRevision(globalId, schema, version, revision);
        // insert information in extra table
        recordService.insertRepresentationRevision(globalId, schema, revision.getRevisionProviderId(),
                revision.getRevisionName(), version, revision.getCreationTimeStamp());

        dataSetService.updateAllRevisionDatasetsEntries(globalId, schema, version, revision);
    }

    private void setRevisionTags(Revision revision, Set<String> tags) {
        if (tags == null || tags.isEmpty()) {
            return;
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
    }

    private <T> ResponseEntity<T> createResponseEntity(HttpServletRequest httpServletRequest, T entity) {
        HttpRequest httpRequest = new ServletServerHttpRequest(httpServletRequest);
        URI resultURI = UriComponentsBuilder.fromHttpRequest(httpRequest).build().toUri();
        ResponseEntity.BodyBuilder bb = ResponseEntity.created(resultURI);
        if (entity != null) {
            return bb.body(entity);
        } else {
            return bb.build();
        }
    }
}
