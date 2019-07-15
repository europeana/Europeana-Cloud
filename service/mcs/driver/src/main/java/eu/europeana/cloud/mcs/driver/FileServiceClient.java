package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.filter.ECloudBasicAuthFilter;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.*;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static eu.europeana.cloud.common.web.ParamConstants.H_RANGE;

/**
 * Exposes API related to files.
 */
public class FileServiceClient extends MCSClient {

    private final Client client;
    private static final Logger logger = LoggerFactory.getLogger(FileServiceClient.class);
    //records/CLOUDID/representations/REPRESENTATIONNAME/versions/VERSION/files/
    private static final String filesPath = "records/{" + ParamConstants.P_CLOUDID + "}/representations/{"
            + ParamConstants.P_REPRESENTATIONNAME + "}/versions/{" + ParamConstants.P_VER + "}/files";
    //records/CLOUDID/representations/REPRESENTATIONNAME/versions/VERSION/files/FILENAME/
    private static final String filePath = filesPath + "/{" + ParamConstants.P_FILENAME + "}";


    /**
     * Constructs a FileServiceClient
     *
     * @param baseUrl url of the MCS Rest Service
     */
    public FileServiceClient(String baseUrl) {
            this(baseUrl, DEFAULT_CONNECT_TIMEOUT_IN_MILLIS, DEFAULT_READ_TIMEOUT_IN_MILLIS);
    }

    public FileServiceClient(String baseUrl, final int connectTimeoutInMillis, final int readTimeoutInMillis) {
        super(baseUrl);
        client = JerseyClientBuilder.newClient().register(MultiPartFeature.class);
        this.client.property(ClientProperties.CONNECT_TIMEOUT, connectTimeoutInMillis);
        this.client.property(ClientProperties.READ_TIMEOUT, readTimeoutInMillis);
    }

    /**
     * Creates instance of FileServiceClient. Same as {@link #FileServiceClient(String)}
     * but includes username and password to perform authenticated requests.
     *
     * @param baseUrl URL of the MCS Rest Service
     */
    public FileServiceClient(String baseUrl, final String username, final String password) {
        this(baseUrl, username, password, DEFAULT_CONNECT_TIMEOUT_IN_MILLIS, DEFAULT_READ_TIMEOUT_IN_MILLIS);
    }

    public FileServiceClient(String baseUrl, final String username, final String password,
                             final int connectTimeoutInMillis, final int readTimeoutInMillis) {
        super(baseUrl);
        client = JerseyClientBuilder.newClient()
                .register(MultiPartFeature.class)
                .register(HttpAuthenticationFeature.basicBuilder().credentials(username, password).build());
        this.client.property(ClientProperties.CONNECT_TIMEOUT, connectTimeoutInMillis);
        this.client.property(ClientProperties.READ_TIMEOUT, readTimeoutInMillis);
    }

    /**
     * Function returns file content.
     *
     * @param cloudId            id of returned file.
     * @param representationName representation name of returned file.
     * @param version            version of returned file.
     * @param fileName           name of file.
     * @return InputStream returned content.
     * @throws RepresentationNotExistsException when requested representation (or representation version) does not exist.
     * @throws FileNotExistsException           when requested file does not exist.
     * @throws DriverException                  call to service has not succeeded because of server side error.
     * @throws MCSException                     on unexpected situations.
     */
    public InputStream getFile(String cloudId, String representationName, String version, String fileName)
            throws RepresentationNotExistsException, FileNotExistsException, DriverException, MCSException, IOException {
        WebTarget target = client.target(baseUrl).path(filePath).resolveTemplate(ParamConstants.P_CLOUDID, cloudId)
                .resolveTemplate(ParamConstants.P_REPRESENTATIONNAME, representationName)
                .resolveTemplate(ParamConstants.P_VER, version).resolveTemplate(ParamConstants.P_FILENAME, fileName);
        Builder requset = target.request();

        Response response = null;

        try {
            response = requset.get();
            return handleReadFileResponse(response);
        } finally {
            closeResponse(response);
        }
    }


    /**
     * Function returns file content. By setting range parameter one can retrieve only a part of content.
     *
     * @param cloudId            id of returned file.
     * @param representationName representation name of returned file.
     * @param version            version of returned file.
     * @param fileName           name of file.
     * @param range              range of bytes to return. Range header can be found in Hypertext Transfer Protocol HTTP/1.1, section
     *                           14.35 Range).
     * @return InputStream returned content.
     * @throws RepresentationNotExistsException when requested representation (or representation version) does not exist.
     * @throws FileNotExistsException           when requested file does not exist.
     * @throws WrongContentRangeException       when wrong value in "Range" header.
     * @throws DriverException                  call to service has not succeeded because of server side error.
     * @throws MCSException                     on unexpected situations.
     */
    public InputStream getFile(String cloudId, String representationName, String version, String fileName, String range)
            throws RepresentationNotExistsException, FileNotExistsException, WrongContentRangeException,
            DriverException, MCSException, IOException {
        WebTarget target = client.target(baseUrl).path(filePath).resolveTemplate(ParamConstants.P_CLOUDID, cloudId)
                .resolveTemplate(ParamConstants.P_REPRESENTATIONNAME, representationName)
                .resolveTemplate(ParamConstants.P_VER, version).resolveTemplate(ParamConstants.P_FILENAME, fileName);
        Builder request = target.request().header(H_RANGE, range);

        Response response = null;

        try {
            response = request.get();
            if (response.getStatus() == Response.Status.PARTIAL_CONTENT.getStatusCode()) {
                InputStream contentResponse = response.readEntity(InputStream.class);
                return copiedInputStream(contentResponse);
            } else {
                ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
                throw MCSExceptionProvider.generateException(errorInfo);
            }

        } finally {
            closeResponse(response);
        }
    }

    /**
     * Function returns file content.
     */
    public InputStream getFile(String fileUrl)
            throws RepresentationNotExistsException, FileNotExistsException, WrongContentRangeException,
            DriverException, MCSException, IOException {

        Response response = null;

        try {
            response = client.target(fileUrl).request().get();

            return handleReadFileResponse(response);
        } finally {
            closeResponse(response);
        }
    }

    private InputStream handleReadFileResponse(Response response) throws IOException, MCSException {
        if (response.getStatus() == Status.OK.getStatusCode()) {
            InputStream contentResponse = response.readEntity(InputStream.class);
            return copiedInputStream(contentResponse);
        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }

    public InputStream getFile(String fileUrl,String key,String value)
            throws RepresentationNotExistsException, FileNotExistsException, WrongContentRangeException,
            DriverException, MCSException, IOException {

        Response response = null;

        try {
            response = client.target(fileUrl).request().header(key,value).get();

            return handleReadFileResponse(response);
        } finally {
            closeResponse(response);
        }
    }

    /**
     * Uploads file content with checking checksum.
     *
     * @param cloudId            id of uploaded file.
     * @param representationName representation name of uploaded file.
     * @param version            version of uploaded file.
     * @param data               InputStream (content) of uploaded file.
     * @param mediaType          mediaType of uploaded file.
     * @param expectedMd5        expected MD5 checksum.
     * @return URI to uploaded file.
     * @throws IOException                                   when incorrect MD5 checksum.
     * @throws RepresentationNotExistsException              when representation does not exist in specified version.
     * @throws CannotModifyPersistentRepresentationException when specified representation version is persistent and modifying its files is not allowed.
     * @throws DriverException                               call to service has not succeeded because of server side error.
     * @throws MCSException                                  on unexpected situations.
     */
    public URI uploadFile(String cloudId, String representationName, String version, InputStream data,
                          String mediaType, String expectedMd5)
            throws IOException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
            DriverException, MCSException {
        Response response = null;
        FormDataMultiPart multipart = new FormDataMultiPart();
        try {
            WebTarget target = client.target(baseUrl).path(filesPath).resolveTemplate(ParamConstants.P_CLOUDID, cloudId)
                    .resolveTemplate(ParamConstants.P_REPRESENTATIONNAME, representationName)
                    .resolveTemplate(ParamConstants.P_VER, version);
            multipart.field(ParamConstants.F_FILE_MIME, mediaType).field(
                    ParamConstants.F_FILE_DATA, data, MediaType.APPLICATION_OCTET_STREAM_TYPE);
            Builder request = target.request();
            response = request.post(Entity.entity(multipart, multipart.getMediaType()));

            return handleResponse(expectedMd5, response, Status.CREATED.getStatusCode());
        } finally {
            closeOpenResources(data, multipart, response);
        }
    }

    private URI handleResponse(String expectedMd5, Response response, int expectedStatusCode) throws IOException, MCSException {
        if (response.getStatus() == expectedStatusCode) {
            if (!expectedMd5.equals(response.getEntityTag().getValue())) {
                throw new IOException("Incorrect MD5 checksum");
            }
            return response.getLocation();
        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }


    /**
     * Uploads file content without checking checksum.
     *
     * @param cloudId            id of uploaded file.
     * @param representationName representation name of uploaded file.
     * @param version            version of uploaded file.
     * @param data               InputStream (content) of uploaded file.
     * @param mediaType          mediaType of uploaded file.
     * @return URI of uploaded file.
     * @throws RepresentationNotExistsException              when representation does not exist in specified version.
     * @throws CannotModifyPersistentRepresentationException when specified representation version is persistent and modifying its files is not allowed.
     * @throws DriverException                               call to service has not succeeded because of server side error.
     * @throws MCSException                                  on unexpected situations.
     */
    public URI uploadFile(String cloudId, String representationName, String version, InputStream data, String mediaType)
            throws IOException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException, DriverException,
            MCSException {

        Response response = null;
        FormDataMultiPart multipart = new FormDataMultiPart();
        try {
            WebTarget target = client.target(baseUrl).path(filesPath).resolveTemplate(ParamConstants.P_CLOUDID, cloudId)
                    .resolveTemplate(ParamConstants.P_REPRESENTATIONNAME, representationName)
                    .resolveTemplate(ParamConstants.P_VER, version);
            multipart.field(ParamConstants.F_FILE_MIME, mediaType).field(
                    ParamConstants.F_FILE_DATA, data, MediaType.APPLICATION_OCTET_STREAM_TYPE);
            Builder request = target.request();
            response = request.post(Entity.entity(multipart, multipart.getMediaType()));

            if (response.getStatus() == Status.CREATED.getStatusCode()) {
                return response.getLocation();
            } else {
                ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
                throw MCSExceptionProvider.generateException(errorInfo);
            }

        } finally {
            closeOpenResources(data, multipart, response);
        }
    }


    /**
     * Uploads file content without checking checksum.
     *
     * @param cloudId            id of uploaded file.
     * @param representationName representation name of uploaded file.
     * @param version            version of uploaded file.
     * @param data               InputStream (content) of uploaded file.
     * @param mediaType          mediaType of uploaded file.
     * @param fileName           user file name
     * @return URI of uploaded file.
     * @throws RepresentationNotExistsException              when representation does not exist in specified version.
     * @throws CannotModifyPersistentRepresentationException when specified representation version is persistent and modifying its files is not allowed.
     * @throws DriverException                               call to service has not succeeded because of server side error.
     * @throws MCSException                                  on unexpected situations.
     */
    public URI uploadFile(String cloudId, String representationName, String version, String fileName, InputStream data, String mediaType)
            throws IOException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException, DriverException,
            MCSException {
        Response response = null;
        FormDataMultiPart multipart = new FormDataMultiPart();
        try {
            WebTarget target = client.target(baseUrl).path(filesPath).resolveTemplate(ParamConstants.P_CLOUDID, cloudId)
                    .resolveTemplate(ParamConstants.P_REPRESENTATIONNAME, representationName)
                    .resolveTemplate(ParamConstants.P_VER, version);
            multipart.field(ParamConstants.F_FILE_MIME, mediaType).field(
                    ParamConstants.F_FILE_DATA, data, MediaType.APPLICATION_OCTET_STREAM_TYPE).field(ParamConstants.F_FILE_NAME, fileName);
            Invocation.Builder request = target.request();
            response = request.post(Entity.entity(multipart, multipart.getMediaType()));

            if (response.getStatus() == Response.Status.CREATED.getStatusCode()) {
                return response.getLocation();
            } else {
                ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
                throw MCSExceptionProvider.generateException(errorInfo);
            }
        } finally {
            closeOpenResources(data, multipart, response);
        }
    }

    /**
     * Uploads file content without checking checksum.
     *
     * @param versionUrl Path to the version where the file will be uploaded to.
     *                   For example:
     *                   "http://ecloud.eanadev.org:8080/ecloud-service-mcs/records/L9WSPSMVQ85/representations/edm/versions/b17c4f60-70d0-11e4-8fe1-00163eefc9c8"
     */
    public URI uploadFile(String versionUrl, InputStream data, String mediaType)
            throws IOException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException, DriverException,
            MCSException {

        String filesPath = "/files";

        Response response = null;
        FormDataMultiPart multipart = new FormDataMultiPart();
        try {
            multipart.field(ParamConstants.F_FILE_MIME, mediaType).field(
                    ParamConstants.F_FILE_DATA, data, MediaType.APPLICATION_OCTET_STREAM_TYPE);

            response = client.target(versionUrl + filesPath).request().post(Entity.entity(multipart, multipart.getMediaType()));

            if (response.getStatus() == Status.CREATED.getStatusCode()) {
                return response.getLocation();
            } else {
                ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
                throw MCSExceptionProvider.generateException(errorInfo);
            }

        } finally {
            closeOpenResources(data, multipart, response);
        }
    }

    /**
     * Modifies existed file with checking checksum.
     *
     * @param cloudId            id of modifying file.
     * @param representationName representation name of modifying file.
     * @param version            version of modifying file.
     * @param data               InputStream (content) of modifying file.
     * @param mediaType          mediaType of modifying file.
     * @param fileName           name of modifying file.
     * @param expectedMd5        expected MD5 checksum.
     * @return URI to modified file.
     * @throws IOException                                   when checksum is incorrect.
     * @throws RepresentationNotExistsException              when representation does not exist in specified version.
     * @throws CannotModifyPersistentRepresentationException when specified representation version is persistent and modifying its files is not allowed.
     * @throws DriverException                               call to service has not succeeded because of server side error.
     * @throws MCSException                                  on unexpected situations.
     */
    public URI modyfiyFile(String cloudId, String representationName, String version, InputStream data,
                           String mediaType, String fileName, String expectedMd5)
            throws IOException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
            DriverException, MCSException {

        Response response = null;
        FormDataMultiPart multipart = new FormDataMultiPart();

        try {
            WebTarget target = client.target(baseUrl).path(filePath).resolveTemplate(ParamConstants.P_CLOUDID, cloudId)
                    .resolveTemplate(ParamConstants.P_REPRESENTATIONNAME, representationName)
                    .resolveTemplate(ParamConstants.P_VER, version).resolveTemplate(ParamConstants.P_FILENAME, fileName);
            multipart.field(ParamConstants.F_FILE_MIME, mediaType).field(
                    ParamConstants.F_FILE_DATA, data, MediaType.APPLICATION_OCTET_STREAM_TYPE);
            response = target.request().put(Entity.entity(multipart, multipart.getMediaType()));

            return handleResponse(expectedMd5, response, Status.NO_CONTENT.getStatusCode());

        } finally {
            closeOpenResources(data, multipart, response);
        }
    }

    public URI modifyFile(String fileUrl, InputStream data, String mediaType)
            throws IOException, RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
            DriverException, MCSException {

        WebTarget target = client.target(fileUrl);

        FormDataMultiPart multipart = new FormDataMultiPart();
        Response response = null;
        try {
            multipart.field(ParamConstants.F_FILE_MIME, mediaType).field(
                    ParamConstants.F_FILE_DATA, data, MediaType.APPLICATION_OCTET_STREAM_TYPE);

            response = target.request().put(Entity.entity(multipart, multipart.getMediaType()));

            if (response.getStatus() == Status.NO_CONTENT.getStatusCode()) {
                return response.getLocation();

            } else {
                ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
                throw MCSExceptionProvider.generateException(errorInfo);
            }

        } finally {
            closeOpenResources(data, multipart, response);
        }
    }

    private void closeOpenResources(InputStream data, FormDataMultiPart multipart, Response response) throws IOException {
        closeResponse(response);
        IOUtils.closeQuietly(data);
        multipart.close();
    }


    /**
     * Deletes existed file.
     *
     * @param cloudId            id of deleting file.
     * @param representationName representation name of deleting file.
     * @param version            version of deleting file.
     * @param fileName           name of deleting file.
     * @throws RepresentationNotExistsException              when representation does not exist in specified version.
     * @throws FileNotExistsException                        when requested file does not exist.
     * @throws CannotModifyPersistentRepresentationException when specified representation version is persistent and modifying its files is not allowed.
     * @throws DriverException                               call to service has not succeeded because of server side error.
     * @throws MCSException                                  on unexpected situations.
     */
    public void deleteFile(String cloudId, String representationName, String version, String fileName)
            throws RepresentationNotExistsException, FileNotExistsException,
            CannotModifyPersistentRepresentationException, DriverException, MCSException {
        WebTarget target = client.target(baseUrl).path(filePath).resolveTemplate(ParamConstants.P_CLOUDID, cloudId)
                .resolveTemplate(ParamConstants.P_REPRESENTATIONNAME, representationName)
                .resolveTemplate(ParamConstants.P_VER, version).resolveTemplate(ParamConstants.P_FILENAME, fileName);

        Response response = null;
        try {
            response = target.request().delete();
            if (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
                ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
                throw MCSExceptionProvider.generateException(errorInfo);
            }
        } finally {
            closeResponse(response);
        }
    }

    /**
     * Retrieve file uri from parameters.
     *
     * @param cloudId            id of file.
     * @param representationName representation name of file.
     * @param version            version of file.
     * @param fileName           name of file.
     * @return file URI
     */
    public URI getFileUri(String cloudId, String representationName, String version, String fileName) {
        WebTarget target = client.target(baseUrl).path(filePath)
                .resolveTemplate(ParamConstants.P_CLOUDID, cloudId)
                .resolveTemplate(ParamConstants.P_REPRESENTATIONNAME, representationName)
                .resolveTemplate(ParamConstants.P_VER, version).resolveTemplate(ParamConstants.P_FILENAME, fileName);

        return target.getUri();
    }

    /**
     * Retrieve parts of file uri.
     * <p>
     * Examples:
     * From this string "http://ecloud.eanadev.org:8080/ecloud-service-mcs/records/L9WSPSMVQ85/representations/edm/versions/b17c4f60-70d0/files"
     * Retrieve: {"CLOUDID": "L9WSPSMVQ85",
     * "REPRESENTATIONNAME": "edm",
     * "VERSION": "b17c4f60-70d0",
     * "FILENAME": null
     * }
     * <p/>
     * From this string "http://ecloud.eanadev.org:8080/ecloud-service-mcs/records/L9WSPSMVQ85/representations/edm/versions/b17c4f60-70d0/files/file1"
     * Retrieve: {"CLOUDID": "L9WSPSMVQ85",
     * "REPRESENTATIONNAME": "edm",
     * "VERSION": "b17c4f60-70d0",
     * "FILENAME": "file1"
     * }
     * </p>
     *
     * @param uri Address of file/files
     * @return Map with indexes: CLOUDID, REPRESENTATIONNAME, VERSION, FILENAME(potentially null)
     */
    public static Map<String, String> parseFileUri(String uri) {
        Pattern p = Pattern.compile(".*/records/([^/]+)/representations/([^/]+)/versions/([^/]+)/files/(.*)");
        Matcher m = p.matcher(uri);

        if (m.find()) {
            Map<String, String> ret = new HashMap<>();
            ret.put(ParamConstants.P_CLOUDID, m.group(1));
            ret.put(ParamConstants.P_REPRESENTATIONNAME, m.group(2));
            ret.put(ParamConstants.P_VER, m.group(3));
            ret.put(ParamConstants.P_FILENAME, m.group(4));

            return ret;
        } else {
            return null;
        }
    }

    /**
     * Client will use provided authorization header for all requests;
     *
     * @param headerValue authorization header value
     * @return
     */
    public void useAuthorizationHeader(final String headerValue) {
        client.register(new ECloudBasicAuthFilter(headerValue));
    }

    private InputStream copiedInputStream(InputStream originIS) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int nRead;
        byte[] data = new byte[16384];
        while ((nRead = originIS.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }
        buffer.flush();
        IOUtils.closeQuietly(originIS);
        return new ByteArrayInputStream(buffer.toByteArray());
    }

    private void closeResponse(Response response) {
        if (response != null) {
            response.close();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        client.close();
    }

    public void close() {
        client.close();
    }
}
