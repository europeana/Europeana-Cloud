package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.web.ParamConstants;
import static eu.europeana.cloud.common.web.ParamConstants.H_RANGE;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;

import java.io.IOException;
import java.io.InputStream;

import java.net.URI;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileServiceClient {

    private final Client client;
    private final String baseUrl;
    private static final Logger logger = LoggerFactory.getLogger(FileServiceClient.class);
    //records/CLOUDID/representations/REPRESENTATIONNAME/versions/VERSION/files/
    private static final String filesPath = "records/{" + ParamConstants.P_CLOUDID + "}/representations/{"
            + ParamConstants.P_REPRESENTATIONNAME + "}/versions/{" + ParamConstants.P_VER + "}/files";
    //records/CLOUDID/representations/REPRESENTATIONNAME/versions/VERSION/files/FILENAME/
    private static final String filePath = filesPath + "/" + ParamConstants.P_FILENAME;


    /**
     * Constructs a FileServiceClient
     * 
     * @param baseUrl
     *            url of the MCS Rest Service
     */
    public FileServiceClient(String baseUrl) {
        client = JerseyClientBuilder.newClient().register(MultiPartFeature.class);
        this.baseUrl = baseUrl;
    }


    /**
     * Function returns file content.
     * 
     * @param fileName
     *            name of file.
     * @param cloudId
     *            id of returned file.
     * @param schema
     *            schema of returned file.
     * @param version
     *            version of returned file.
     * @param range
     *            range of bytes to return (optional). Range header can be found in Hypertext Transfer Protocol
     *            HTTP/1.1, section 14.35 Range).
     * @return InputStream returned content.
     * @throws RepresentationNotExistsException
     *             when requested representation (or representation version) does not exist.
     * @throws FileNotExistsException
     *             when requested file does not exist.
     * @throws WrongContentRangeException
     *             when wrong value in "Range" header.
     * @throws MCSException
     *             on unexpected situations.
     */
    public InputStream getFile(String fileName, String cloudId, String schema, String version, String range)
            throws RepresentationNotExistsException, FileNotExistsException, WrongContentRangeException, MCSException {
        WebTarget target = client.target(baseUrl).path(filePath).resolveTemplate(ParamConstants.P_CLOUDID, cloudId)
                .resolveTemplate(ParamConstants.P_REPRESENTATIONNAME, schema)
                .resolveTemplate(ParamConstants.P_VER, version).resolveTemplate(ParamConstants.P_FILENAME, fileName);
        Builder requset;

        if (range != null)
            requset = target.request().header(H_RANGE, range);
        else
            requset = target.request();

        Response response = requset.get();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            InputStream contentResponse = response.readEntity(InputStream.class);
            return contentResponse;
        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }


    /**
     * Returns file content with checking checksum.
     * 
     * @param cloudId
     *            id of uploaded file.
     * @param schema
     *            schema of uploaded file.
     * @param version
     *            version of uploaded file.
     * @param data
     *            InputStream of uploaded file.
     * @param mediaType
     *            mediaType of uploaded file.
     * @param expectedMd5
     *            expected MD5 checksum.
     * @return URI of uploaded file.
     * @throws IOException
     * @throws RepresentationNotExistsException
     *             when representation does not exist in specified version.
     * @throws CannotModifyPersistentRepresentationException
     *             when specified representation version is persistent and modifying its files is not allowed.
     * @throws MCSException
     *             on unexpected situations.
     */
    public URI uploadFile(String cloudId, String schema, String version, InputStream data, String mediaType,
            String expectedMd5)
            throws IOException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
            MCSException {
        WebTarget target = client.target(baseUrl).path(filesPath).resolveTemplate(ParamConstants.P_CLOUDID, cloudId)
                .resolveTemplate(ParamConstants.P_REPRESENTATIONNAME, schema)
                .resolveTemplate(ParamConstants.P_VER, version);
        FormDataMultiPart multipart = new FormDataMultiPart().field(ParamConstants.F_FILE_MIME, mediaType).field(
            ParamConstants.F_FILE_DATA, data, MediaType.APPLICATION_OCTET_STREAM_TYPE);
        Builder request = target.request();

        Response response = request.post(Entity.entity(multipart, multipart.getMediaType()));

        if (response.getStatus() == Status.CREATED.getStatusCode()) {
            if (expectedMd5.equals(response.getEntityTag().getValue()))
                throw new IOException("Incorrect MD5 checksum");
            return response.getLocation();
        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }


    /**
     * Returns file content without checking checksum.
     * 
     * @param cloudId
     *            id of uploaded file.
     * @param schema
     *            schema of uploaded file.
     * @param version
     *            version of uploaded file.
     * @param data
     *            InputStream of uploaded file.
     * @param mediaType
     *            mediaType of uploaded file.
     * @return URI of uploaded file.
     * @throws IOException
     * @throws RepresentationNotExistsException
     *             when representation does not exist in specified version.
     * @throws CannotModifyPersistentRepresentationException
     *             when specified representation version is persistent and modifying its files is not allowed.
     * @throws MCSException
     *             on unexpected situations.
     */
    public URI uploadFile(String cloudId, String schema, String version, InputStream data, String mediaType)
            throws IOException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
            MCSException {
        WebTarget target = client.target(baseUrl).path(filesPath).resolveTemplate(ParamConstants.P_CLOUDID, cloudId)
                .resolveTemplate(ParamConstants.P_REPRESENTATIONNAME, schema)
                .resolveTemplate(ParamConstants.P_VER, version);
        FormDataMultiPart multipart = new FormDataMultiPart().field(ParamConstants.F_FILE_MIME, mediaType).field(
            ParamConstants.F_FILE_DATA, data, MediaType.APPLICATION_OCTET_STREAM_TYPE);
        Builder request = target.request();

        Response response = request.post(Entity.entity(multipart, multipart.getMediaType()));

        if (response.getStatus() == Status.CREATED.getStatusCode()) {
            return response.getLocation();
        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }


    /**
     * Modifies existed file with checking checksum.
     * 
     * @param cloudId
     *            id of modifying file.
     * @param schema
     *            schema of modifying file.
     * @param version
     *            version of modifying file.
     * @param data
     *            InputStream of modifying file.
     * @param mediaType
     *            mediaType of modifying file.
     * @param fileName
     *            name of modifying file.
     * @param expectedMd5
     *            expected MD5 checksum.
     * @return URI to modified file.
     * @throws IOException
     *             when checksum is incorrect.
     * @throws RepresentationNotExistsException
     *             when representation does not exist in specified version.
     * @throws CannotModifyPersistentRepresentationException
     *             when specified representation version is persistent and modifying its files is not allowed.
     * @throws MCSException
     *             on unexpected situations.
     */
    public URI modyfiyFile(String cloudId, String schema, String version, InputStream data, String mediaType,
            String fileName, String expectedMd5)
            throws IOException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
            MCSException {
        WebTarget target = client.target(baseUrl).path(filePath).resolveTemplate(ParamConstants.P_CLOUDID, cloudId)
                .resolveTemplate(ParamConstants.P_REPRESENTATIONNAME, schema)
                .resolveTemplate(ParamConstants.P_VER, version).resolveTemplate(ParamConstants.P_FILENAME, fileName);
        FormDataMultiPart multipart = new FormDataMultiPart().field(ParamConstants.F_FILE_MIME, mediaType).field(
            ParamConstants.F_FILE_DATA, data, MediaType.APPLICATION_OCTET_STREAM_TYPE);
        Response response = target.request().put(Entity.entity(multipart, multipart.getMediaType()));
        if (response.getStatus() == Status.NO_CONTENT.getStatusCode()) {
            if (expectedMd5.equals(response.getEntityTag().getValue()))
                throw new IOException("Incorrect MD5 checksum");
            return response.getLocation();

        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }


    /**
     * Deletes existed file.
     * 
     * @param cloudId
     *            id of deleting file.
     * @param schema
     *            schema of deleting file.
     * @param version
     *            version of deleting file.
     * @param fileName
     *            name of deleting file.
     * @throws RepresentationNotExistsException
     *             when representation does not exist in specified version.
     * @throws FileNotExistsException
     *             when requested file does not exist.
     * @throws CannotModifyPersistentRepresentationException
     *             when specified representation version is persistent and modifying its files is not allowed.
     * @throws MCSException
     *             on unexpected situations.
     */
    public void deleteFile(String cloudId, String schema, String version, String fileName)
            throws RepresentationNotExistsException, FileNotExistsException,
            CannotModifyPersistentRepresentationException, MCSException {
        WebTarget target = client.target(baseUrl).path(filePath).resolveTemplate(ParamConstants.P_CLOUDID, cloudId)
                .resolveTemplate(ParamConstants.P_REPRESENTATIONNAME, schema)
                .resolveTemplate(ParamConstants.P_VER, version).resolveTemplate(ParamConstants.P_FILENAME, fileName);
        Response response = target.request().delete();
        if (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }

}
