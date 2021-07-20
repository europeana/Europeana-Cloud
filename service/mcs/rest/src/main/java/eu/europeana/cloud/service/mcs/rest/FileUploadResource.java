package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.aas.acl.ExtendedAclService;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.Storage;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.utils.EnrichUriUtil;
import eu.europeana.cloud.service.mcs.utils.storage_selector.PreBufferedInputStream;
import eu.europeana.cloud.service.mcs.utils.storage_selector.StorageSelector;
import org.apache.commons.io.IOUtils;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.FILE_UPLOAD_RESOURCE;

/**
 * Handles uploading the file when representation is not created yet.
 */
@RestController
@RequestMapping(FILE_UPLOAD_RESOURCE)
public class FileUploadResource {

    private static final String REPRESENTATION_CLASS_NAME = Representation.class.getName();
    private final RecordService recordService;
    private final ExtendedAclService aclService;
    private final Integer objectStoreSizeThreshold;

    public FileUploadResource(
            RecordService recordService,
            ExtendedAclService aclService,
            Integer objectStoreSizeThreshold) {
        this.recordService = recordService;
        this.aclService = aclService;
        this.objectStoreSizeThreshold = objectStoreSizeThreshold;
    }

    /**
     * 
     * Creates representation, uploads file and persists this representation in one request
     * @param httpServletRequest
     * @param cloudId      cloudId
     * @param representationName        representation name
     * @param fileName      file name
     * @param providerId    providerId
     * @param mimeType      mimeType of uploaded file
     * @param data          uploaded file content
     * @return
     * @throws RepresentationNotExistsException
     * @throws CannotModifyPersistentRepresentationException
     * @throws RecordNotExistsException
     * @throws ProviderNotExistsException
     * @throws FileAlreadyExistsException
     * @throws CannotPersistEmptyRepresentationException
     * 
     * @summary Upload file for non existing representation
     */
    @PostMapping(consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
    @PreAuthorize("isAuthenticated()")
    public ResponseEntity<?> sendFile(
            HttpServletRequest httpServletRequest,
            @PathVariable String cloudId,
            @PathVariable String representationName,
            @RequestParam(required = false) UUID version,
            @RequestParam String fileName,
            @RequestParam String providerId,
            @RequestParam String mimeType ,
            @RequestParam MultipartFile data) throws RepresentationNotExistsException,
            CannotModifyPersistentRepresentationException, RecordNotExistsException,
            ProviderNotExistsException, FileAlreadyExistsException, CannotPersistEmptyRepresentationException, IOException {

        PreBufferedInputStream prebufferedInputStream = new PreBufferedInputStream(data.getInputStream(), objectStoreSizeThreshold);
        Storage storage = new StorageSelector(prebufferedInputStream, mimeType).selectStorage();

        Representation representation = recordService.createRepresentation(cloudId, representationName, providerId, version);
        addPrivilegesToRepresentation(representation);

        File file = addFileToRepresentation(representation, prebufferedInputStream, mimeType, fileName, storage);
        persistRepresentation(representation);

        EnrichUriUtil.enrich(httpServletRequest, cloudId, representationName, representation.getVersion(), file);

        return ResponseEntity
                .created(file.getContentUri())
                .eTag(file.getMd5())
                .build();
    }
    
    private void addPrivilegesToRepresentation(Representation representation) {
        String creatorName = SpringUserUtils.getUsername();

        if (creatorName != null) {
            ObjectIdentity versionIdentity = new ObjectIdentityImpl(
                    REPRESENTATION_CLASS_NAME,
                    representation.getCloudId() + "/" + representation.getRepresentationName() + "/" + representation.getVersion());

            MutableAcl versionAcl = aclService.insertOrUpdateAcl(versionIdentity);

            versionAcl.insertAce(0, BasePermission.READ, new PrincipalSid(creatorName), true);
            versionAcl.insertAce(1, BasePermission.WRITE, new PrincipalSid(creatorName), true);
            versionAcl.insertAce(2, BasePermission.DELETE, new PrincipalSid(creatorName), true);
            versionAcl.insertAce(3, BasePermission.ADMINISTRATION, new PrincipalSid(creatorName), true);

            aclService.updateAcl(versionAcl);
        }
    }

    private File addFileToRepresentation(
            Representation representation, InputStream data,  String mimeType, String fileName, Storage storage)
                throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException {

        File f = new File();
        f.setMimeType(mimeType);
        f.setFileStorage(storage);

        if (fileName == null) {
            fileName = UUID.randomUUID().toString();
        }
        f.setFileName(fileName);
        recordService.putContent(representation.getCloudId(), representation.getRepresentationName(),
                representation.getVersion(), f, data);
        IOUtils.closeQuietly(data);
        return f;
    }

    private void persistRepresentation(Representation representation) throws
            CannotModifyPersistentRepresentationException,
            CannotPersistEmptyRepresentationException,
            RepresentationNotExistsException {
        recordService.persistRepresentation(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion());
    }
}
