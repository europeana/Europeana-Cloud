package eu.europeana.cloud.service.mcs.rest;

import static eu.europeana.cloud.common.web.ParamConstants.F_FILE_DATA;
import static eu.europeana.cloud.common.web.ParamConstants.F_FILE_MIME;
import static eu.europeana.cloud.common.web.ParamConstants.F_FILE_NAME;
import static eu.europeana.cloud.common.web.ParamConstants.P_CLOUDID;
import static eu.europeana.cloud.common.web.ParamConstants.P_REPRESENTATIONNAME;
import static eu.europeana.cloud.common.web.ParamConstants.P_VER;

import java.io.InputStream;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.glassfish.jersey.media.multipart.FormDataParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.service.mcs.RecordService;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;

/**
 * FilesResource
 */
@Path("/records/{" + P_CLOUDID + "}/representations/{" + P_REPRESENTATIONNAME + "}/versions/{" + P_VER + "}/files")
@Component
@Scope("request")
public class FilesResource {

    @Autowired
    private RecordService recordService;

    @Context
    private UriInfo uriInfo;

    @PathParam(P_CLOUDID)
    private String globalId;

    @PathParam(P_REPRESENTATIONNAME)
    private String schema;

    @PathParam(P_VER)
    private String version;

    private static final Logger LOGGER = LoggerFactory.getLogger("RequestsLogger");


    /**
     * Adds a new file to representation version. URI to created resource will be returned in response as content
     * location. Consumes multipart content - form data:
     * <ul>
     * <li>{@value eu.europeana.cloud.common.web.ParamConstants#F_FILE_MIME} - file mime type</li>
     * <li>{@value eu.europeana.cloud.common.web.ParamConstants#F_FILE_NAME} - file name</li>
     * <li>{@value eu.europeana.cloud.common.web.ParamConstants#F_FILE_DATA} - binary stream of file content (required)</li>
     * </ul>
     * 
     * @param mimeType
     *            mime type of file
     * @param data
     *            binary stream of file content (required)
     * @param fileName
     *            name of creating file. If fileName does not provided by POST request fileName will assigned
     *            automatically by service.
     * @return empty response with tag (content md5) and URI to created resource in content location.
     * @statuscode 201 object has been created.
     * @throws RepresentationNotExistsException
     *             representation does not exist in specified version
     * @throws CannotModifyPersistentRepresentationException
     *             specified representation version is persistent and modifying its files is not allowed.
     * @throws FileAlreadyExistsException
     *             specified file already exist.
     */
    @POST
    @Consumes(MediaType.MULTIPART_FORM_DATA)
    public Response sendFile(@FormDataParam(F_FILE_MIME) String mimeType, @FormDataParam(F_FILE_DATA) InputStream data,
            @FormDataParam(F_FILE_NAME) String fileName)
            throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
            FileAlreadyExistsException {
        ParamUtil.require(F_FILE_DATA, data);

        File f = new File();
        f.setMimeType(mimeType);
        if (fileName != null) {

            try {
                File temp = recordService.getFile(globalId, schema, version, fileName);
                if (temp != null) {
                    throw new FileAlreadyExistsException(fileName);
                }
            } catch (FileNotExistsException e) {
                //expected exception
            }

            f.setFileName(fileName);
        }
        recordService.putContent(globalId, schema, version, f, data);

        EnrichUriUtil.enrich(uriInfo, globalId, schema, version, f);
        LOGGER.debug(String.format("File added [%s, %s, %s], uri: %s ", globalId, schema, version, f.getContentUri()));
        return Response.created(f.getContentUri()).tag(f.getMd5()).build();
    }
}
