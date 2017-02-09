package eu.europeana.cloud.service.mcs.rest;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.aas.authentication.SpringUserUtils;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.Storage;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.rest.storage.selector.PreBufferedInputStream;
import eu.europeana.cloud.service.mcs.rest.storage.selector.StorageSelector;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.stereotype.Component;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.InputStream;
import java.util.UUID;

import static eu.europeana.cloud.common.web.ParamConstants.*;
import static eu.europeana.cloud.service.mcs.rest.storage.selector.PreBufferedInputStream.wrap;

/**
 * Handles uploading the file when representation is not created yet.
 */
@Path("/records/{" + P_CLOUDID + "}/representations/{" + P_REPRESENTATIONNAME + "}/files")
@Component
@Scope("request")
public class FileUploadResource {

    private final int OBJECT_STORE_SIZE_TRESHOLD = 512 * 1024;

    @Autowired
    private RecordService recordService;

    @Autowired
    private MutableAclService mutableAclService;

    private final String REPRESENTATION_CLASS_NAME = Representation.class
            .getName();

    /**
     * 
     * Creates representation, uploads file and persists this representation in one request
     * @param uriInfo
     * @param globalId      cloudId
     * @param schema        representation name
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
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    @PreAuthorize("isAuthenticated()")
    public Response sendFile(@Context UriInfo uriInfo,
                             @PathParam(P_CLOUDID) String globalId,
                             @PathParam(P_REPRESENTATIONNAME) String schema,
                             @FormDataParam(F_FILE_NAME) String fileName,
                             @FormDataParam(F_PROVIDER) String providerId,
                             @FormDataParam(F_FILE_MIME) String mimeType,
                             @FormDataParam(F_FILE_DATA) InputStream data)
            throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
            FileNotExistsException, RecordNotExistsException, ProviderNotExistsException, FileAlreadyExistsException,
            AccessDeniedOrObjectDoesNotExistException, CannotPersistEmptyRepresentationException {
        ParamUtil.require(F_FILE_NAME,fileName);
        ParamUtil.require(F_PROVIDER,providerId);
        ParamUtil.require(F_FILE_DATA, data);
        ParamUtil.require(F_FILE_MIME, mimeType);

        PreBufferedInputStream prebufferedInputStream = wrap(data, OBJECT_STORE_SIZE_TRESHOLD);
        Storage storage = new StorageSelector(prebufferedInputStream, mimeType).selectStorage();

        Representation representation = null;
        representation = recordService.createRepresentation(globalId, schema, providerId);
        addPrivilegesToRepresentation(representation);

        File file = addFileToRepresentation(representation, prebufferedInputStream, mimeType, fileName, storage);
        persistRepresentation(representation);

        EnrichUriUtil.enrich(uriInfo, globalId, schema, representation.getVersion(), file);
        return Response.created(file.getContentUri()).tag(file.getMd5()).build();
    }
    
    private void addPrivilegesToRepresentation(Representation representation) {
        String creatorName = SpringUserUtils.getUsername();

        if (creatorName != null) {

            ObjectIdentity versionIdentity = new ObjectIdentityImpl(
                    REPRESENTATION_CLASS_NAME, representation.getCloudId() + "/" + representation.getRepresentationName() + "/"
                    + representation.getVersion());

            MutableAcl versionAcl = mutableAclService
                    .createAcl(versionIdentity);

            versionAcl.insertAce(0, BasePermission.READ, new PrincipalSid(
                    creatorName), true);
            versionAcl.insertAce(1, BasePermission.WRITE, new PrincipalSid(
                    creatorName), true);
            versionAcl.insertAce(2, BasePermission.DELETE, new PrincipalSid(
                    creatorName), true);
            versionAcl.insertAce(3, BasePermission.ADMINISTRATION,
                    new PrincipalSid(creatorName), true);

            mutableAclService.updateAcl(versionAcl);
        }
    }

    private File addFileToRepresentation(Representation representation, InputStream data,
                                         String mimeType, String fileName, Storage storage)
            throws
            RepresentationNotExistsException,
            FileAlreadyExistsException,
            CannotModifyPersistentRepresentationException {
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
        return f;
    }

    private void persistRepresentation(Representation representation) throws CannotModifyPersistentRepresentationException, CannotPersistEmptyRepresentationException, RepresentationNotExistsException {
        recordService.persistRepresentation(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion());
    }
}
