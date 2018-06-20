package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.filter.ECloudBasicAuthFilter;
import eu.europeana.cloud.common.model.Permission;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.exception.*;
import eu.europeana.cloud.service.mcs.status.McsErrorCode;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.message.internal.MessageBodyProviderNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.*;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.UUID;

import static eu.europeana.cloud.common.web.ParamConstants.*;

/**
 * Exposes API related to records.
 */
public class RecordServiceClient extends MCSClient {

    private final Client client = ClientBuilder.newClient().register(MultiPartFeature.class);
    private static final Logger logger = LoggerFactory.getLogger(RecordServiceClient.class);

    //records/{CLOUDID}
    private static final String recordPath;
    //records/{CLOUDID}/representations
    private static final String representationsPath;
    //records/{CLOUDID}/representations/{REPRESENTATIONNAME}
    private static final String represtationNamePath;
    //records/{CLOUDID}/representations/{REPRESENTATIONNAME}/versions
    private static final String versionsPath;
    //records/{CLOUDID}/representations/{REPRESENTATIONNAME}/versions/{VERSION}
    private static final String versionPath;
    //records/{CLOUDID}/representations/{REPRESENTATIONNAME}/versions/{VERSION}/copy
    private static final String copyPath;
    //records/{CLOUDID}/representations/{REPRESENTATIONNAME}/versions/{VERSION}/persist
    private static final String persistPath;
    //records/{CLOUDID}/representations/{REPRESENTATIONNAME}/versions/{VERSION}/permissions/{TYPE}/users/{USER_NAME}
    private static final String grantingPermissionsToVesionPath;
    //records/{CLOUDID}/representations/{REPRESENTATIONNAME}/versions/{VERSION}/permit
    private static final String permitPath;
    //records/{CLOUDID}/representations/{REPRESENTATIONNAME}/revisions/{REVISIONID}
    private static final String representationsRevisionsPath;

    static {
        StringBuilder builder = new StringBuilder();

        builder.append(ParamConstants.RECORDS);
        builder.append("/");
        builder.append("{");
        builder.append(P_CLOUDID);
        builder.append("}");
        recordPath = builder.toString();

        builder.append("/");
        builder.append(ParamConstants.REPRESENTATIONS);
        representationsPath = builder.toString();

        builder.append("/");
        builder.append("{");
        builder.append(P_REPRESENTATIONNAME);
        builder.append("}");
        represtationNamePath = builder.toString();

        builder.append("/");
        builder.append(ParamConstants.VERSIONS);
        versionsPath = builder.toString();

        builder.append("/");
        builder.append("{");
        builder.append(ParamConstants.P_VER);
        builder.append("}");
        versionPath = builder.toString();

        copyPath = versionPath + "/" + ParamConstants.COPY;
        persistPath = versionPath + "/" + ParamConstants.PERSIST;
        permitPath = versionPath + "/" + ParamConstants.PERMIT;

        grantingPermissionsToVesionPath = versionPath + "/permissions/{" + ParamConstants.P_PERMISSION_TYPE + "}/users/{" + ParamConstants.P_USERNAME + "}";
        representationsRevisionsPath = represtationNamePath + "/revisions/{" + P_REVISION_NAME + "}";
    }

    /**
     * Creates instance of RecordServiceClient.
     *
     * @param baseUrl URL of the MCS Rest Service
     */
    public RecordServiceClient(String baseUrl) {
        super(baseUrl);
    }

    /**
     * Creates instance of RecordServiceClient. Same as {@link #RecordServiceClient(String)}
     * but includes username and password to perform authenticated requests.
     *
     * @param baseUrl URL of the MCS Rest Service
     */
    public RecordServiceClient(String baseUrl, final String username, final String password) {
        this(baseUrl);
        client.register(HttpAuthenticationFeature.basicBuilder().credentials(username, password).build());
    }

    /**
     * Client will use provided authorization header for all requests;
     *
     * @param headerValue authorization header value
     */
    public void useAuthorizationHeader(final String headerValue) {
        client.register(new ECloudBasicAuthFilter(headerValue));
    }

    /**
     * Returns record with all its latest persistent representations.
     *
     * @param cloudId id of the record (required)
     * @return record of specified cloudId (required)
     * @throws RecordNotExistsException when id is not known UIS Service
     * @throws MCSException             on unexpected situations
     */
    public Record getRecord(String cloudId)
            throws RecordNotExistsException, MCSException {

        WebTarget target = client.target(baseUrl).path(recordPath).resolveTemplate(P_CLOUDID, cloudId);
        Builder request = target.request();

        Response response = null;
        try {
            response = request.get();
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                return response.readEntity(Record.class);
            }
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);

        } finally {
            closeResponse(response);
        }

    }

    /**
     * Deletes record with all its representations in all versions.
     * <p/>
     * Does not remove mapping from Unique Identifier Service. If record exists,
     * but nothing was deleted (it had no representations assigned), nothing
     * happens.
     *
     * @param cloudId id of deleted record (required)
     * @throws RecordNotExistsException if cloudId is not known UIS Service
     * @throws MCSException             on unexpected situations
     */
    public void deleteRecord(String cloudId)
            throws RecordNotExistsException, MCSException {

        WebTarget target = client.target(baseUrl).path(recordPath).resolveTemplate(P_CLOUDID, cloudId);
        Builder request = target.request();

        handleDeleteRequest(request);
    }

    /**
     * Lists all latest persistent versions of record representation.
     *
     * @param cloudId id of record from which to get representations (required)
     * @return list of representations
     * @throws RecordNotExistsException if cloudId is not known UIS Service
     * @throws MCSException             on unexpected situations
     */
    public List<Representation> getRepresentations(String cloudId)
            throws RecordNotExistsException, MCSException {

        WebTarget target = client.target(baseUrl).path(representationsPath)
                .resolveTemplate(P_CLOUDID, cloudId);

        Response response = null;
        try {
            response = target.request().get();
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                return response.readEntity(new GenericType<List<Representation>>() {
                }); //formatting here is irreadble
            }

            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);

        } finally {
            closeResponse(response);
        }
    }

    /**
     * Returns latest persistent version of representation.
     *
     * @param cloudId            id of record from which to get representations (required)
     * @param representationName name of the representation (required)
     * @return representation of specified representationName and cloudId
     * @throws RepresentationNotExistsException representation does not exist or
     *                                          no persistent version of this representation exists
     * @throws MCSException                     on unexpected situations
     */
    public Representation getRepresentation(String cloudId, String representationName)
            throws RepresentationNotExistsException, MCSException {
        WebTarget target = client.target(baseUrl).path(represtationNamePath)
                .resolveTemplate(P_CLOUDID, cloudId)
                .resolveTemplate(P_REPRESENTATIONNAME, representationName);
        Builder request = target.request();

        Response response = null;
        try {
            response = request.get();
            if (response.getStatus() == Response.Status.OK.getStatusCode()
                    || response.getStatus() == Response.Status.TEMPORARY_REDIRECT.getStatusCode()) {
                Representation representation = response.readEntity(Representation.class);
                return representation;
            } else {
                ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
                throw MCSExceptionProvider.generateException(errorInfo);
            }

        } finally {
            closeResponse(response);
        }
    }

    /**
     * Creates new representation version.
     *
     * @param cloudId            id of the record in which to create the representation
     *                           (required)
     * @param representationName name of the representation to be created
     *                           (required)
     * @param providerId         provider of this representation version (required)
     * @return URI to the created representation
     * @throws ProviderNotExistsException when no provider with given id exists
     * @throws RecordNotExistsException   when cloud id is not known to UIS
     *                                    Service
     * @throws MCSException               on unexpected situations
     */
    public URI createRepresentation(String cloudId, String representationName, String providerId)
            throws ProviderNotExistsException, RecordNotExistsException, MCSException {

        WebTarget target = client.target(baseUrl).path(represtationNamePath)
                .resolveTemplate(P_CLOUDID, cloudId)
                .resolveTemplate(P_REPRESENTATIONNAME, representationName);
        Builder request = target.request();
        Form form = new Form();
        form.param("providerId", providerId);

        Response response = null;
        return handleRepresentationResponse(form, request, response);
    }

    /**
     * Creates new representation version, aploads a file and makes this representation persistent (in one request)
     *
     * @param cloudId            id of the record in which to create the representation
     *                           (required)
     * @param representationName name of the representation to be created
     *                           (required)
     * @param providerId         provider of this representation version (required)
     * @param data               file that should be uploaded (required)
     * @param fileName           name for created file
     * @param mediaType          mimeType of uploaded file
     * @return URI to created file
     */
    public URI createRepresentation(String cloudId,
                                    String representationName,
                                    String providerId,
                                    InputStream data,
                                    String fileName,
                                    String mediaType) throws IOException, MCSException {
        WebTarget target = client.target(baseUrl).path(represtationNamePath + "/files")
                .resolveTemplate(P_CLOUDID, cloudId)
                .resolveTemplate(P_REPRESENTATIONNAME, representationName);
        Builder request = target.request();

        FormDataMultiPart multipart = null;

        Response response = null;
        request.header("Content-Type", "multipart/form-data");
        try {
            multipart = prepareRequestBody(providerId, data, fileName, mediaType);
            response = request.post(Entity.entity(multipart, MediaType.MULTIPART_FORM_DATA));
            return handleResponse(response);
        } finally {
            closeResponse(response);
            IOUtils.closeQuietly(data);
            if (multipart != null)
                multipart.close();
            if (data!=null)
                data.close();
        }
    }

    /**
     * Creates new representation version, aploads a file and makes this representation persistent (in one request)
     *
     * @param cloudId            id of the record in which to create the representation
     *                           (required)
     * @param representationName name of the representation to be created
     *                           (required)
     * @param providerId         provider of this representation version (required)
     * @param data               file that should be uploaded (required)
     * @param mediaType          mimeType of uploaded file
     * @return URI to created file
     * @throws MCSException
     */
    public URI createRepresentation(String cloudId,
                                    String representationName,
                                    String providerId,
                                    InputStream data,
                                    String mediaType) throws IOException, MCSException {

        return this.createRepresentation(cloudId, representationName, providerId, data, null, mediaType);
    }

    private FormDataMultiPart prepareRequestBody(String providerId, InputStream data, String fileName, String mediaType) {
        FormDataMultiPart requestBody = new FormDataMultiPart();
        requestBody.field(ParamConstants.F_PROVIDER, providerId)
                .field(ParamConstants.F_FILE_DATA, data, MediaType.APPLICATION_OCTET_STREAM_TYPE)
                .field(ParamConstants.F_FILE_MIME, mediaType);


        if (fileName == null || !"".equals(fileName.trim())) {
            fileName = UUID.randomUUID().toString();
        }
        requestBody.field(ParamConstants.F_FILE_NAME, fileName);

        return requestBody;
    }

    /**
     * Deletes representation with all versions.
     *
     * @param cloudId            id of the record to delete representation from (required)
     * @param representationName representation name of deleted representation
     *                           (required)
     * @throws RepresentationNotExistsException if specified Representation does
     *                                          not exist
     * @throws MCSException                     on unexpected situations
     */
    public void deleteRepresentation(String cloudId, String representationName)
            throws RepresentationNotExistsException, MCSException {
        WebTarget target = client.target(baseUrl).path(represtationNamePath)
                .resolveTemplate(P_CLOUDID, cloudId)
                .resolveTemplate(P_REPRESENTATIONNAME, representationName);
        Builder request = target.request();

        handleDeleteRequest(request);
    }

    /**
     * Lists all versions of record representation.
     *
     * @param cloudId            id of the record to get representation from (required)
     * @param representationName name of the representation (required)
     * @return representation versions list
     * @throws RepresentationNotExistsException if specified Representation does
     *                                          not exist
     * @throws MCSException                     on unexpected situations
     */
    public List<Representation> getRepresentations(String cloudId, String representationName)
            throws RepresentationNotExistsException, MCSException {
        WebTarget target = client.target(baseUrl).path(versionsPath).resolveTemplate(P_CLOUDID, cloudId)
                .resolveTemplate(P_REPRESENTATIONNAME, representationName);
        Builder request = target.request();

        Response response = null;
        try {
            response = request.get();
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                List<Representation> list = response.readEntity(new GenericType<List<Representation>>() {
                });
                return list;
            } else {
                ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
                throw MCSExceptionProvider.generateException(errorInfo);
            }
        } finally {
            closeResponse(response);
        }
    }

    /**
     * Returns representation in specified version.
     * <p/>
     * If Version = LATEST, will redirect to actual latest persistent version at
     * the moment of invoking this method.
     *
     * @param cloudId            id of the record to get representation from (required)
     * @param representationName name of the representation (required)
     * @param version            version of the representation to be obtained; if
     *                           version==LATEST function will return latest persistent version (required)
     * @return requested representation version
     * @throws RepresentationNotExistsException if specified representation does
     *                                          not exist
     * @throws MCSException                     on unexpected situations
     */
    public Representation getRepresentation(String cloudId, String representationName, String version)
            throws RepresentationNotExistsException, MCSException {
        WebTarget webtarget = client.target(baseUrl).path(versionPath)
                .resolveTemplate(P_CLOUDID, cloudId)
                .resolveTemplate(P_REPRESENTATIONNAME, representationName)
                .resolveTemplate(ParamConstants.P_VER, version);
        Builder request = webtarget.request();

        Response response = null;
        try {
            response = request.get();
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                Representation representation = response.readEntity(Representation.class);
                return representation;
            } else {
                ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
                throw MCSExceptionProvider.generateException(errorInfo);
            }
        } catch (MessageBodyProviderNotFoundException e) {
            String out = webtarget.getUri().toString();
            throw new MCSException(out, e);
        } finally {
            closeResponse(response);
        }
    }

    /**
     * Deletes representation in specified version.
     *
     * @param cloudId            id of the record to delete representation version from
     *                           (required)
     * @param representationName name of the representation (required)
     * @param version            the deleted version of the representation (required)
     * @throws RepresentationNotExistsException              if specified representation does
     *                                                       not exist
     * @throws CannotModifyPersistentRepresentationException if specified
     *                                                       representation is persistent and thus cannot be removed
     * @throws MCSException                                  on unexpected situations
     */
    public void deleteRepresentation(String cloudId, String representationName, String version)
            throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException, MCSException {
        WebTarget webtarget = client.target(baseUrl).path(versionPath)
                .resolveTemplate(P_CLOUDID, cloudId)
                .resolveTemplate(P_REPRESENTATIONNAME, representationName)
                .resolveTemplate(ParamConstants.P_VER, version);
        Builder request = webtarget.request();

        handleDeleteRequest(request);
    }

    private void handleDeleteRequest(Builder request) throws MCSException {
        Response response = null;
        try {
            response = request.delete();
            if (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
                ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
                throw MCSExceptionProvider.generateException(errorInfo);
            }
        } finally {
            closeResponse(response);
        }
    }

    /**
     * Copies all information from one representation version to another.
     * <p/>
     * Copies all information with all files and their content from one
     * representation version to a new temporary one.
     *
     * @param cloudId            id of the record that holds representation (required)
     * @param representationName name of the copied representation (required)
     * @param version            version of the copied representation (required)
     * @return URI to the created copy of representation
     * @throws RepresentationNotExistsException if specified representation
     *                                          version does not exist
     * @throws MCSException                     on unexpected situations
     */
    public URI copyRepresentation(String cloudId, String representationName, String version)
            throws RepresentationNotExistsException, MCSException {
        WebTarget target = client.target(baseUrl).path(copyPath).resolveTemplate(P_CLOUDID, cloudId)
                .resolveTemplate(P_REPRESENTATIONNAME, representationName)
                .resolveTemplate(ParamConstants.P_VER, version);
        Builder request = target.request();

        Response response = null;
        try {
            response = request.post(Entity.entity(new Form(), MediaType.APPLICATION_FORM_URLENCODED_TYPE));
            if (response.getStatus() == Response.Status.CREATED.getStatusCode()) {
                return response.getLocation();
            } else {
                ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
                throw MCSExceptionProvider.generateException(errorInfo);
            }
        } finally {
            closeResponse(response);
        }
    }

    /**
     * Makes specified temporary representation version persistent.
     *
     * @param cloudId            id of the record that holds representation (required)
     * @param representationName name of the representation to be persisted
     *                           (required)
     * @param version            version that should be made persistent (required)
     * @return URI to the persisted representation
     * @throws RepresentationNotExistsException              when representation does not
     *                                                       exist in specified version
     * @throws CannotModifyPersistentRepresentationException when representation
     *                                                       version is already persistent
     * @throws CannotPersistEmptyRepresentationException     when representation
     *                                                       version has no file attached and thus cannot be made persistent
     * @throws MCSException                                  on unexpected situations
     */
    public URI persistRepresentation(String cloudId, String representationName, String version)
            throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
            CannotPersistEmptyRepresentationException, MCSException {
        WebTarget target = client.target(baseUrl).path(persistPath).resolveTemplate(P_CLOUDID, cloudId)
                .resolveTemplate(P_REPRESENTATIONNAME, representationName)
                .resolveTemplate(ParamConstants.P_VER, version);
        Form form = new Form();
        Builder request = target.request();

        Response response = null;
        return handleRepresentationResponse(form, request, response);
    }

    private URI handleRepresentationResponse(Form form, Builder request, Response response) throws MCSException {
        try {
            response = request.post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));
            return handleResponse(response);
        } finally {
            closeResponse(response);
        }
    }

    private URI handleResponse(Response response) throws MCSException {
        if (response.getStatus() == Response.Status.CREATED.getStatusCode()) {
            URI uri = response.getLocation();
            return uri;
        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }

    /**
     * Adds selected permission(s) to selected representation version.
     *
     * @param cloudId            record identifier
     * @param representationName representation name
     * @param version            representation version
     * @param userName           user who will get access to representation version
     * @param permission         permission that will be granted
     * @throws MCSException
     */
    public void grantPermissionsToVersion(String cloudId, String representationName, String version, String userName, Permission permission) throws MCSException {
        WebTarget target = client.target(baseUrl).path(grantingPermissionsToVesionPath)
                .resolveTemplate(P_CLOUDID, cloudId)
                .resolveTemplate(P_REPRESENTATIONNAME, representationName)
                .resolveTemplate(ParamConstants.P_VER, version)
                .resolveTemplate(ParamConstants.P_PERMISSION_TYPE, permission.getValue())
                .resolveTemplate(ParamConstants.P_USERNAME, userName);

        Builder request = target.request();

        Response response = null;
        try {
            response = request.post(null);
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throwException(response);
            }
        } finally {
            closeResponse(response);
        }
    }

    /**
     * Revokes permission(s) to selected representation version.
     *
     * @param cloudId            record identifier
     * @param representationName representation name
     * @param version            representation version
     * @param userName           user who will get access to representation version
     * @param permission         permission that will be granted
     * @throws MCSException
     */
    public void revokePermissionsToVersion(String cloudId, String representationName, String version, String userName, Permission permission) throws MCSException {
        WebTarget target = client.target(baseUrl).path(grantingPermissionsToVesionPath)
                .resolveTemplate(P_CLOUDID, cloudId)
                .resolveTemplate(P_REPRESENTATIONNAME, representationName)
                .resolveTemplate(ParamConstants.P_VER, version)
                .resolveTemplate(ParamConstants.P_PERMISSION_TYPE, permission.getValue())
                .resolveTemplate(ParamConstants.P_USERNAME, userName);

        Builder request = target.request();

        handleDeleteRequest(request);
    }

    /**
     * Adds selected permission(s) to selected representation version.
     *
     * @param cloudId            record identifier
     * @param representationName representation name
     * @param version            representation version
     * @throws MCSException
     */
    public void permitVersion(String cloudId, String representationName, String version) throws MCSException {
        WebTarget target = client.target(baseUrl).path(permitPath)
                .resolveTemplate(P_CLOUDID, cloudId)
                .resolveTemplate(P_REPRESENTATIONNAME, representationName)
                .resolveTemplate(ParamConstants.P_VER, version);

        Builder request = target.request();

        Response response = null;
        try {
            response = request.post(null);
            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throwException(response);
            }
        } finally {
            closeResponse(response);
        }
    }

    private void throwException(Response response) throws MCSException {
        try {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        } catch (MessageBodyProviderNotFoundException e) {
            ErrorInfo errorInfo = new ErrorInfo();
            errorInfo.setErrorCode(McsErrorCode.OTHER.toString());
            errorInfo.setDetails("Mcs not available");
            throw MCSExceptionProvider.generateException(errorInfo);
        }
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


    /**
     * Returns representation in specified version.
     * <p/>
     * If Version = LATEST, will redirect to actual latest persistent version at
     * the moment of invoking this method.
     *
     * @param cloudId            id of the record to get representation from (required)
     * @param representationName name of the representation (required)
     * @param revisionName       revision name (required)
     * @param revisionProviderId revision provider identifier, together with revisionId it is used to determine the correct revision (required)
     * @return requested representation version
     * @throws RepresentationNotExistsException if specified representation does
     *                                          not exist
     * @throws MCSException                     on unexpected situations
     */
    public RepresentationRevisionResponse getRepresentationRevision(String cloudId, String representationName, String revisionName, String revisionProviderId, String revisionTimestamp)
            throws RevisionNotExistsException, MCSException {
        WebTarget webtarget = client.target(baseUrl).path(representationsRevisionsPath)
                .resolveTemplate(P_CLOUDID, cloudId)
                .resolveTemplate(P_REPRESENTATIONNAME, representationName)
                .resolveTemplate(P_REVISION_NAME, revisionName);

        if (revisionProviderId != null) {
            webtarget = webtarget.queryParam(F_REVISION_PROVIDER_ID, revisionProviderId);
        } else
            throw new MCSException("RevisionProviderId is required");
        // revision timestamp is optional
        if (revisionTimestamp != null) {
            webtarget = webtarget.queryParam(F_REVISION_TIMESTAMP, revisionTimestamp);
        }

        Builder request = webtarget.request();

        Response response = null;
        try {
            response = request.get();
            if (response.getStatus() == Response.Status.OK.getStatusCode()) {
                RepresentationRevisionResponse representationRevisionResponse = response.readEntity(RepresentationRevisionResponse.class);
                return representationRevisionResponse;
            } else {
                ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
                throw MCSExceptionProvider.generateException(errorInfo);
            }
        } catch (MessageBodyProviderNotFoundException e) {
            String out = webtarget.getUri().toString();
            throw new MCSException(out, e);
        } finally {
            closeResponse(response);
        }
    }
}
