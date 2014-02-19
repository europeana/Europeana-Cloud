package eu.europeana.cloud.mcs.driver;

import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.web.ParamConstants;
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
     * @return InputStream
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
        WebTarget target = client.target(this.baseUrl)
                .path("records/{ID}/representations/{SCHEMA}/versions/{VERSION}/files/{FILE_NAME}")
                .resolveTemplate("MCS_SERVICE_ADDRES", baseUrl).resolveTemplate("ID", cloudId)
                .resolveTemplate("SCHEMA", schema).resolveTemplate("VERSION", version)
                .resolveTemplate("FILE_NAME", fileName);
        Builder requset;
        if (range != null)
            requset = target.request().header("Range", range);
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
     *             when specified representation version is persistent and modyfying its files is not allowed.
     * @throws MCSException
     *             on unexpected situations.
     */
    public URI uploadFile(String cloudId, String schema, String version, InputStream data, String mediaType)
            throws IOException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
            MCSException {
        WebTarget target = client.target(this.baseUrl)
                .path("records/{ID}/representations/{SCHEMA}/versions/{VERSION}/files/")
                .resolveTemplate("MCS_SERVICE_ADDRES", baseUrl).resolveTemplate("ID", cloudId)
                .resolveTemplate("SCHEMA", schema).resolveTemplate("VERSION", version);
        FormDataMultiPart multipart = new FormDataMultiPart().field(ParamConstants.F_FILE_MIME, mediaType).field(
            ParamConstants.F_FILE_DATA, data, MediaType.APPLICATION_OCTET_STREAM_TYPE);
        Builder request = target.request();
        Response response = request.post(Entity.entity(multipart, multipart.getMediaType()));
        if (response.getStatus() == Status.CREATED.getStatusCode()) {
            String resultMd5 = response.getEntityTag().getValue();
            byte[] contentByte = ByteStreams.toByteArray(data);
            String contentMd5 = Hashing.md5().hashBytes(contentByte).toString();
            if (contentMd5.equals(resultMd5)) {
                return response.getLocation();
            } else {
                ErrorInfo errorInfo = new ErrorInfo("000", "Incorrect md5 chcecsum");
                throw MCSExceptionProvider.generateException(errorInfo);
            }
        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }


    /**
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
     * @return URI to modified file.
     * @throws IOException
     * @throws RepresentationNotExistsException
     * @throws CannotModifyPersistentRepresentationException
     * @throws MCSException
     */
    public URI modyfiyFile(String cloudId, String schema, String version, InputStream data, String mediaType,
            String fileName)
            throws IOException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
            MCSException {
        WebTarget target = client.target(this.baseUrl)
                .path("records/{ID}/representations/{SCHEMA}/versions/{VERSION}/files/{FILE}/")
                .resolveTemplate("MCS_SERVICE_ADDRES", baseUrl).resolveTemplate("ID", cloudId)
                .resolveTemplate("SCHEMA", schema).resolveTemplate("VERSION", version)
                .resolveTemplate("FILE", fileName);
        FormDataMultiPart multipart = new FormDataMultiPart().field(ParamConstants.F_FILE_MIME, mediaType).field(
            ParamConstants.F_FILE_DATA, data, MediaType.APPLICATION_OCTET_STREAM_TYPE);
        Response response = target.request().put(Entity.entity(multipart, multipart.getMediaType()));
        if (response.getStatus() == Status.NO_CONTENT.getStatusCode()) {
            String resultMd5 = response.getEntityTag().getValue();
            byte[] contentByte = ByteStreams.toByteArray(data);
            String contentMd5 = Hashing.md5().hashBytes(contentByte).toString();
            if (contentMd5.equals(resultMd5)) {
                return response.getLocation();
            } else {
                ErrorInfo errorInfo = new ErrorInfo("000", "Incorrect md5 chcecsum");
                throw MCSExceptionProvider.generateException(errorInfo);
            }
        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }

    }


    public void deleteFile(String cloudId, String schema, String versionId, String fileName) {

    }

}
