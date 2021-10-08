package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.filter.ECloudBasicAuthFilter;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.*;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;

import javax.ws.rs.client.*;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static eu.europeana.cloud.common.web.ParamConstants.*;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.CLIENT_FILE_RESOURCE;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.FILES_RESOURCE;

/**
 * Exposes API related to files.
 */
public class FileServiceClient extends MCSClient {

    private final Client client = ClientBuilder.newBuilder()
            .register(JacksonFeature.class)
            .register(MultiPartFeature.class)
            .build();

    /**
     * Constructs a FileServiceClient
     *
     * @param baseUrl url of the MCS Rest Service
     */
    public FileServiceClient(String baseUrl) {
        this(baseUrl, null, null);
    }

    /**
     * Creates instance of FileServiceClient. Same as {@link #FileServiceClient(String)}
     * but includes username and password to perform authenticated requests.
     *
     * @param baseUrl URL of the MCS Rest Service
     */
    public FileServiceClient(String baseUrl, final String username, final String password) {
        this(baseUrl, null, username, password, DEFAULT_CONNECT_TIMEOUT_IN_MILLIS, DEFAULT_READ_TIMEOUT_IN_MILLIS);
    }

    public FileServiceClient(String baseUrl, final String authorizationHeader) {
        this(baseUrl,  authorizationHeader, null, null, DEFAULT_CONNECT_TIMEOUT_IN_MILLIS, DEFAULT_READ_TIMEOUT_IN_MILLIS);
    }

    /**
     * All parameters constructor used by another one
     * @param baseUrl URL of the MCS Rest Service
     * @param authorizationHeader Authorization header - used instead username/password pair
     * @param username Username to HTTP authorisation  (use together with password)
     * @param password Password to HTTP authorisation (use together with username)
     * @param connectTimeoutInMillis Timeout for waiting for connecting
     * @param readTimeoutInMillis Timeout for getting data
     */
    public FileServiceClient(String baseUrl, final String authorizationHeader,
                             final String username,  final String password,
                             final int connectTimeoutInMillis, final int readTimeoutInMillis) {

        super(baseUrl);

        if (authorizationHeader != null) {
            client.register(new ECloudBasicAuthFilter(authorizationHeader));
        } else if(username != null || password != null) {
            client.register(HttpAuthenticationFeature.basicBuilder().credentials(username, password).build());
        }

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
    public InputStream getFile(String cloudId, String representationName,
                               String version, String fileName) throws MCSException, IOException {

        WebTarget target = client
                .target(baseUrl)
                .path(CLIENT_FILE_RESOURCE)
                .resolveTemplate(CLOUD_ID, cloudId)
                .resolveTemplate(REPRESENTATION_NAME, representationName)
                .resolveTemplate(VERSION, version)
                .resolveTemplate(FILE_NAME, fileName);
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
    public InputStream getFile(String cloudId, String representationName, String version,
                               String fileName, String range)  throws MCSException, IOException {

        WebTarget target = client
                .target(baseUrl)
                .path(CLIENT_FILE_RESOURCE)
                .resolveTemplate(CLOUD_ID, cloudId)
                .resolveTemplate(REPRESENTATION_NAME, representationName)
                .resolveTemplate(VERSION, version)
                .resolveTemplate(FILE_NAME, fileName);
        Builder request = target.request().header(H_RANGE, range);

        Response response = null;

        try {
            response = request.get();
            if (response.getStatus() == Response.Status.PARTIAL_CONTENT.getStatusCode()) {
                InputStream contentResponse = response.readEntity(InputStream.class);
                return copiedInputStream(contentResponse);
            } else {
                throw MCSExceptionProvider.generateException(getErrorInfo(response));
            }

        } finally {
            closeResponse(response);
        }
    }

    /**
     * Function returns file content.
     */
    public InputStream getFile(String fileUrl) throws MCSException, IOException {

        Response response = null;

        try {
            response = client.target(fileUrl).request().get();

            return handleReadFileResponse(response);
        } finally {
            closeResponse(response);
        }
    }

    public InputStream getFile(String fileUrl,String key,String value) throws MCSException, IOException {

        Response response = null;

        try {
            response = client
                    .target(fileUrl)
                    .request()
                    .header(key,value)
                    .get();

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
    public URI uploadFile(String cloudId, String representationName, String version,
                          InputStream data, String mediaType, String expectedMd5) throws IOException, MCSException {

        Response response = null;
        FormDataMultiPart multipart = new FormDataMultiPart();
        try {
            WebTarget target = client
                    .target(baseUrl)
                    .path(FILES_RESOURCE)
                    .resolveTemplate(CLOUD_ID, cloudId)
                    .resolveTemplate(REPRESENTATION_NAME, representationName)
                    .resolveTemplate(VERSION, version);

            multipart
                    .field(ParamConstants.F_FILE_MIME, mediaType)
                    .bodyPart(new StreamDataBodyPart(ParamConstants.F_FILE_DATA, data, MediaType.APPLICATION_OCTET_STREAM));

            Builder request = target.request();
            response = request.post(Entity.entity(multipart, multipart.getMediaType()));

            return handleResponse(expectedMd5, response, Status.CREATED.getStatusCode());
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
     * @return URI of uploaded file.
     * @throws RepresentationNotExistsException              when representation does not exist in specified version.
     * @throws CannotModifyPersistentRepresentationException when specified representation version is persistent and modifying its files is not allowed.
     * @throws DriverException                               call to service has not succeeded because of server side error.
     * @throws MCSException                                  on unexpected situations.
     */
    public URI uploadFile(String cloudId, String representationName, String version,
                          InputStream data, String mediaType) throws IOException, MCSException {

        Response response = null;
        FormDataMultiPart multipart = new FormDataMultiPart();
        try {
            WebTarget target = client
                    .target(baseUrl)
                    .path(FILES_RESOURCE)
                    .resolveTemplate(CLOUD_ID, cloudId)
                    .resolveTemplate(REPRESENTATION_NAME, representationName)
                    .resolveTemplate(VERSION, version);

            multipart
                    .field(ParamConstants.F_FILE_MIME, mediaType)
                    .bodyPart(new StreamDataBodyPart(ParamConstants.F_FILE_DATA, data, MediaType.APPLICATION_OCTET_STREAM));

            Builder request = target.request();
            response = request.post(Entity.entity(multipart, multipart.getMediaType()));

            if (response.getStatus() == Status.CREATED.getStatusCode()) {
                return response.getLocation();
            } else {
                throw MCSExceptionProvider.generateException(getErrorInfo(response));
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
    public URI uploadFile(String cloudId, String representationName, String version, String fileName,
                                InputStream data, String mediaType) throws IOException, MCSException {

        Response response = null;
        FormDataMultiPart multipart = new FormDataMultiPart();
        try {
            WebTarget target = client
                    .target(baseUrl)
                    .path(FILES_RESOURCE)
                    .resolveTemplate(CLOUD_ID, cloudId)
                    .resolveTemplate(REPRESENTATION_NAME, representationName)
                    .resolveTemplate(VERSION, version);

            multipart
                    .field(ParamConstants.F_FILE_MIME, mediaType)
                    .field(ParamConstants.F_FILE_NAME, fileName)
                    .bodyPart(new StreamDataBodyPart(ParamConstants.F_FILE_DATA, data, MediaType.APPLICATION_OCTET_STREAM));

            Invocation.Builder request = target.request();
            response = request.post(Entity.entity(multipart, multipart.getMediaType()));

            if (response.getStatus() == Response.Status.CREATED.getStatusCode()) {
                return response.getLocation();
            } else {
                throw MCSExceptionProvider.generateException(getErrorInfo(response));
            }
        } finally {
            closeOpenResources(data, multipart, response);
        }
    }

    /**
     * @deprecated
     * Uploads file content without checking checksum.
     *
     * @param versionUrl Path to the version where the file will be uploaded to.
     *                   For example:
     *                   "http://ecloud.eanadev.org:8080/ecloud-service-mcs/records/L9WSPSMVQ85/representations/edm/versions/b17c4f60-70d0-11e4-8fe1-00163eefc9c8"
     */
    @Deprecated
    public URI uploadFile(String versionUrl, InputStream data, String mediaType) throws IOException, MCSException {

        final String FILES_SEGMENT = "/files";

        Response response = null;
        FormDataMultiPart multipart = new FormDataMultiPart();
        try {
            multipart
                    .field(ParamConstants.F_FILE_MIME, mediaType)
                    .bodyPart(new StreamDataBodyPart(ParamConstants.F_FILE_DATA, data, MediaType.APPLICATION_OCTET_STREAM));

            response = client.target(versionUrl + FILES_SEGMENT).request().post(Entity.entity(multipart, multipart.getMediaType()));

            if (response.getStatus() == Status.CREATED.getStatusCode()) {
                return response.getLocation();
            } else {
                throw MCSExceptionProvider.generateException(getErrorInfo(response));
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
    public URI modyfiyFile(String cloudId, String representationName, String version,
                           InputStream data, String mediaType, String fileName, String expectedMd5) throws IOException, MCSException {

        Response response = null;
        FormDataMultiPart multipart = new FormDataMultiPart();

        try {
            WebTarget target = client
                    .target(baseUrl)
                    .path(CLIENT_FILE_RESOURCE)
                    .resolveTemplate(CLOUD_ID, cloudId)
                    .resolveTemplate(REPRESENTATION_NAME, representationName)
                    .resolveTemplate(VERSION, version)
                    .resolveTemplate(FILE_NAME, fileName);

            response = target.request().put(Entity.entity(data, mediaType));

            return handleResponse(expectedMd5, response, Status.NO_CONTENT.getStatusCode());

        } finally {
            closeOpenResources(data, multipart, response);
        }
    }

    public URI modifyFile(String fileUrl, InputStream data, String mediaType) throws IOException, MCSException {

        WebTarget target = client.target(fileUrl);

        FormDataMultiPart multipart = new FormDataMultiPart();
        Response response = null;
        try {


            response = target.request().put(Entity.entity(data, mediaType));

            if (response.getStatus() == Status.NO_CONTENT.getStatusCode()) {
                return response.getLocation();

            } else {
                throw MCSExceptionProvider.generateException(getErrorInfo(response));
            }

        } finally {
            closeOpenResources(data, multipart, response);
        }
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
    public void deleteFile(String cloudId, String representationName, String version, String fileName) throws MCSException {

        WebTarget target = client
                .target(baseUrl)
                .path(CLIENT_FILE_RESOURCE)
                .resolveTemplate(CLOUD_ID, cloudId)
                .resolveTemplate(REPRESENTATION_NAME, representationName)
                .resolveTemplate(VERSION, version)
                .resolveTemplate(FILE_NAME, fileName);

        Response response = null;
        try {
            response = target.request().delete();
            if (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
                throw MCSExceptionProvider.generateException(getErrorInfo(response));
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
        WebTarget target = client
                .target(baseUrl)
                .path(CLIENT_FILE_RESOURCE)
                .resolveTemplate(CLOUD_ID, cloudId)
                .resolveTemplate(REPRESENTATION_NAME, representationName)
                .resolveTemplate(VERSION, version)
                .resolveTemplate(FILE_NAME, fileName);

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
            Map<String, String> result = new HashMap<>();
            result.put(CLOUD_ID, m.group(1));
            result.put(REPRESENTATION_NAME, m.group(2));
            result.put(VERSION, m.group(3));
            result.put(FILE_NAME, m.group(4));

            return result;
        } else {
            return null;
        }
    }

    /**
     * Client will use provided authorization header for all requests;
     *
     * @param authorizationHeader authorization header value
     */
    public void useAuthorizationHeader(final String authorizationHeader) {
        client.register(new ECloudBasicAuthFilter(authorizationHeader));
    }

    public void close() {
        client.close();
    }

    private void closeResponse(Response response) {
        if (response != null) {
            response.close();
        }
    }

    private void closeOpenResources(InputStream data, FormDataMultiPart multipart, Response response) throws IOException {
        closeResponse(response);
        IOUtils.closeQuietly(data);
        multipart.close();
    }

    private URI handleResponse(String expectedMd5, Response response, int expectedStatusCode) throws IOException, MCSException {
        if (response.getStatus() == expectedStatusCode) {
            if (!expectedMd5.equals(response.getEntityTag().getValue())) {
                throw new IOException("Incorrect MD5 checksum");
            }
            return response.getLocation();
        } else {
            throw MCSExceptionProvider.generateException(getErrorInfo(response));
        }
    }

    private InputStream handleReadFileResponse(Response response) throws IOException, MCSException {
        if (response.getStatus() == Status.OK.getStatusCode()) {
            InputStream contentResponse = response.readEntity(InputStream.class);
            return copiedInputStream(contentResponse);
        } else {
            throw MCSExceptionProvider.generateException(getErrorInfo(response));
        }
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
}
