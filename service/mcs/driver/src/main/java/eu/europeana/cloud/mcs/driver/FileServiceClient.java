package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.filter.ECloudBasicAuthFilter;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.mcs.exception.*;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import static eu.europeana.cloud.common.web.ParamConstants.*;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.CLIENT_FILE_RESOURCE;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.FILES_RESOURCE;

/**
 * Exposes API related for files.
 */
public class FileServiceClient extends MCSClient {

    private static final String IO_EXCEPTION_MESSAGE = "Some I/O problem occurs";

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
        this(baseUrl, authorizationHeader, null, null, DEFAULT_CONNECT_TIMEOUT_IN_MILLIS, DEFAULT_READ_TIMEOUT_IN_MILLIS);
    }

    /**
     * All parameters' constructor used by another one
     *
     * @param baseUrl                URL of the MCS Rest Service
     * @param authorizationHeader    Authorization header - used instead username/password pair
     * @param username               Username to HTTP authorisation  (use together with password)
     * @param password               Password to HTTP authorisation (use together with username)
     * @param connectTimeoutInMillis Timeout for waiting for connecting
     * @param readTimeoutInMillis    Timeout for getting data
     */
    public FileServiceClient(String baseUrl, final String authorizationHeader,
                             final String username, final String password,
                             final int connectTimeoutInMillis, final int readTimeoutInMillis) {

        super(baseUrl);

        if (authorizationHeader != null) {
            client.register(new ECloudBasicAuthFilter(authorizationHeader));
        } else if (username != null || password != null) {
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
                               String version, String fileName) throws MCSException {

        return manageResponse(new ResponseParams<>(InputStream.class),
                () -> client
                        .target(baseUrl)
                        .path(CLIENT_FILE_RESOURCE)
                        .resolveTemplate(CLOUD_ID, cloudId)
                        .resolveTemplate(REPRESENTATION_NAME, representationName)
                        .resolveTemplate(VERSION, version)
                        .resolveTemplate(FILE_NAME, fileName)
                        .request()
                        .get()
        );
    }

    /**
     * Function returns file content. By setting range parameter one can retrieve only a part of content.
     *
     * @param cloudId            id of returned file.
     * @param representationName representation name of returned file.
     * @param version            version of returned file.
     * @param fileName           name of file.
     * @param range              range of bytes to return. Range header can be found in Hypertext Transfer Protocol HTTP/1.1, section
     *                           14.35 Range.
     * @return InputStream returned content.
     * @throws RepresentationNotExistsException when requested representation (or representation version) does not exist.
     * @throws FileNotExistsException           when requested file does not exist.
     * @throws WrongContentRangeException       when wrong value in "Range" header.
     * @throws DriverException                  call to service has not succeeded because of server side error.
     * @throws MCSException                     on unexpected situations.
     */
    public InputStream getFile(String cloudId, String representationName, String version,
                               String fileName, String range) throws MCSException {

        return manageResponse(new ResponseParams<>(InputStream.class, Response.Status.PARTIAL_CONTENT),
                () -> client
                        .target(baseUrl)
                        .path(CLIENT_FILE_RESOURCE)
                        .resolveTemplate(CLOUD_ID, cloudId)
                        .resolveTemplate(REPRESENTATION_NAME, representationName)
                        .resolveTemplate(VERSION, version)
                        .resolveTemplate(FILE_NAME, fileName)
                        .request()
                        .header(H_RANGE, range)
                        .get()
        );
    }

    /**
     * Function returns file content.
     */
    public InputStream getFile(String fileUrl) throws MCSException {
        return manageResponse(new ResponseParams<>(InputStream.class),
                () -> client
                        .target(fileUrl)
                        .request()
                        .get()
        );
    }

    public InputStream getFile(String fileUrl, String key, String value) throws MCSException {
        return manageResponse(new ResponseParams<>(InputStream.class),
                () -> client
                        .target(fileUrl)
                        .request()
                        .header(key, value)
                        .get()
        );
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
     * @throws RepresentationNotExistsException              when representation does not exist in specified version.
     * @throws CannotModifyPersistentRepresentationException when specified representation version is persistent and modifying its files is not allowed.
     * @throws DriverException                               call to service has not succeeded because of server side error.
     * @throws MCSException                                  on unexpected situations.
     */
    public URI uploadFile(String cloudId, String representationName, String version,
                          InputStream data, String mediaType, String expectedMd5) throws MCSException {

        try (data; var multiPart = new FormDataMultiPart()) {
            multiPart
                    .field(ParamConstants.F_FILE_MIME, mediaType)
                    .bodyPart(new StreamDataBodyPart(ParamConstants.F_FILE_DATA, data, MediaType.APPLICATION_OCTET_STREAM));

            return manageResponse(new ResponseParams<>(URI.class, Status.CREATED, expectedMd5),
                    () -> client
                            .target(baseUrl)
                            .path(FILES_RESOURCE)
                            .resolveTemplate(CLOUD_ID, cloudId)
                            .resolveTemplate(REPRESENTATION_NAME, representationName)
                            .resolveTemplate(VERSION, version)
                            .request()
                            .post(Entity.entity(multiPart, multiPart.getMediaType()))
            );
        } catch(IOException ioException) {
            throw MCSExceptionProvider.createException(IO_EXCEPTION_MESSAGE, ioException);
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
                          InputStream data, String mediaType) throws MCSException {

        try (data; var multiPart = new FormDataMultiPart()) {
            multiPart
                    .field(ParamConstants.F_FILE_MIME, mediaType)
                    .bodyPart(new StreamDataBodyPart(ParamConstants.F_FILE_DATA, data, MediaType.APPLICATION_OCTET_STREAM));

            return manageResponse(new ResponseParams<>(URI.class, Status.CREATED),
                    () -> client
                            .target(baseUrl)
                            .path(FILES_RESOURCE)
                            .resolveTemplate(CLOUD_ID, cloudId)
                            .resolveTemplate(REPRESENTATION_NAME, representationName)
                            .resolveTemplate(VERSION, version)
                            .request()
                            .post(Entity.entity(multiPart, multiPart.getMediaType()))
            );
        } catch (IOException ioException){
            throw MCSExceptionProvider.createException(IO_EXCEPTION_MESSAGE, ioException);
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
                          InputStream data, String mediaType) throws MCSException {

        try (data; var multiPart = new FormDataMultiPart()) {
            multiPart
                    .field(ParamConstants.F_FILE_MIME, mediaType)
                    .field(ParamConstants.F_FILE_NAME, fileName)
                    .bodyPart(new StreamDataBodyPart(ParamConstants.F_FILE_DATA, data, MediaType.APPLICATION_OCTET_STREAM));

            return manageResponse(new ResponseParams<>(URI.class, Status.CREATED),
                    () -> client
                            .target(baseUrl)
                            .path(FILES_RESOURCE)
                            .resolveTemplate(CLOUD_ID, cloudId)
                            .resolveTemplate(REPRESENTATION_NAME, representationName)
                            .resolveTemplate(VERSION, version)
                            .request()
                            .post(Entity.entity(multiPart, multiPart.getMediaType()))
            );
        } catch(IOException ioException) {
            throw MCSExceptionProvider.createException(IO_EXCEPTION_MESSAGE, ioException);
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
     * @throws RepresentationNotExistsException              when representation does not exist in specified version.
     * @throws CannotModifyPersistentRepresentationException when specified representation version is persistent and modifying its files is not allowed.
     * @throws DriverException                               call to service has not succeeded because of server side error.
     * @throws MCSException                                  on unexpected situations.
     */
    public URI modifyFile(String cloudId, String representationName, String version,
                           InputStream data, String mediaType, String fileName, String expectedMd5) throws MCSException {

        try(data) {
            return manageResponse(new ResponseParams<>(URI.class, Status.NO_CONTENT, expectedMd5),
                    () -> client
                            .target(baseUrl)
                            .path(CLIENT_FILE_RESOURCE)
                            .resolveTemplate(CLOUD_ID, cloudId)
                            .resolveTemplate(REPRESENTATION_NAME, representationName)
                            .resolveTemplate(VERSION, version)
                            .resolveTemplate(FILE_NAME, fileName)
                            .request()
                            .put(Entity.entity(data, mediaType))
            );
        } catch(IOException ioException) {
            throw MCSExceptionProvider.createException(IO_EXCEPTION_MESSAGE, ioException);
        }
    }

    public URI modifyFile(String fileUrl, InputStream data, String mediaType) throws MCSException {
        try(data) {
            return manageResponse(new ResponseParams<>(URI.class, Status.NO_CONTENT),
                    () -> client
                            .target(fileUrl)
                            .request()
                            .put(Entity.entity(data, mediaType))
            );
        } catch (IOException ioException) {
            throw MCSExceptionProvider.createException(IO_EXCEPTION_MESSAGE, ioException);
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
        manageResponse(new ResponseParams<>(Void.class, Status.NO_CONTENT),
                () -> client
                        .target(baseUrl)
                        .path(CLIENT_FILE_RESOURCE)
                        .resolveTemplate(CLOUD_ID, cloudId)
                        .resolveTemplate(REPRESENTATION_NAME, representationName)
                        .resolveTemplate(VERSION, version)
                        .resolveTemplate(FILE_NAME, fileName)
                        .request()
                        .delete()
        );
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
}
