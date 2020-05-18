package eu.europeana.cloud.service.mcs.rest;

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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.InputStream;
import java.util.UUID;

import static eu.europeana.cloud.common.web.ParamConstants.CLOUD_ID;
import static eu.europeana.cloud.common.web.ParamConstants.REPRESENTATION_NAME;
import static eu.europeana.cloud.service.mcs.utils.storage_selector.PreBufferedInputStream.wrap;

/**
 * Handles uploading the file when representation is not created yet.
 */
@RestController
@RequestMapping("/records/{"+CLOUD_ID+"}/representations/{"+REPRESENTATION_NAME+"}/files")
@Scope("request")
public class FileUploadResource {

    private static final String REPRESENTATION_CLASS_NAME = Representation.class.getName();

    @Autowired
    private RecordService recordService;

    @Autowired
    private MutableAclService mutableAclService;

    @Autowired
    private Integer objectStoreSizeThreshold;

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
     * @throws FileNotExistsException
     * @throws RecordNotExistsException
     * @throws ProviderNotExistsException
     * @throws FileAlreadyExistsException
     * @throws AccessDeniedOrObjectDoesNotExistException
     * @throws CannotPersistEmptyRepresentationException
     * 
     * @summary Upload file for non existing representation
     */
    @PostMapping(consumes = {MediaType.MULTIPART_FORM_DATA_VALUE})
    @PreAuthorize("isAuthenticated()")
    public ResponseEntity<?> sendFile(
            HttpServletRequest httpServletRequest,
            @PathVariable(CLOUD_ID) String cloudId,
            @PathVariable(REPRESENTATION_NAME) String representationName,
            @RequestParam String fileName,
            @RequestParam String providerId,
            @RequestParam String mimeType ,
            @RequestParam byte[] data) throws RepresentationNotExistsException,
                                CannotModifyPersistentRepresentationException, RecordNotExistsException,
                        ProviderNotExistsException, FileAlreadyExistsException, CannotPersistEmptyRepresentationException {

        PreBufferedInputStream prebufferedInputStream = wrap(data, objectStoreSizeThreshold);
        Storage storage = new StorageSelector(prebufferedInputStream, mimeType).selectStorage();

        Representation representation = null;
        representation = recordService.createRepresentation(cloudId, representationName, providerId);
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

            MutableAcl versionAcl = mutableAclService.createAcl(versionIdentity);

            versionAcl.insertAce(0, BasePermission.READ, new PrincipalSid(creatorName), true);
            versionAcl.insertAce(1, BasePermission.WRITE, new PrincipalSid(creatorName), true);
            versionAcl.insertAce(2, BasePermission.DELETE, new PrincipalSid(creatorName), true);
            versionAcl.insertAce(3, BasePermission.ADMINISTRATION, new PrincipalSid(creatorName), true);

            mutableAclService.updateAcl(versionAcl);
        }
    }

    private File addFileToRepresentation(
            Representation representation, InputStream data,  String mimeType, String fileName, Storage storage)
                throws RepresentationNotExistsException, FileAlreadyExistsException, CannotModifyPersistentRepresentationException {

        File f = new File();
        f.setMimeType(mimeType);
        f.setFileStorage(storage);

        if (fileName != null) {
            try {
                File temp = recordService.getFile(representation.getCloudId(),
                        representation.getRepresentationName(), representation.getVersion(),
                        fileName);
                if (temp != null) {
                    throw new FileAlreadyExistsException(fileName);
                }
            } catch (FileNotExistsException e) {
                // file does not exist, so continue and add it
            }
        }

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
