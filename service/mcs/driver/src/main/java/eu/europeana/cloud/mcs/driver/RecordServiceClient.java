package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ErrorInfo;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import java.net.URI;
import java.util.List;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordServiceClient {

    private final String baseUrl;
    private final Client client = ClientBuilder.newClient();
    private static final Logger logger = LoggerFactory.getLogger(DataSetServiceClient.class);

    //records/{ID}
    private static final String recordPath;
    //records/{ID}/representations
    private static final String representationsPath;
    //records/{ID}/representations/{SCHEMA}
    private static final String schemaPath;
    //records/{ID}/representations/{SCHEMA}/versions
    private static final String versionsPath;
    //records/{ID}/representations/{SCHEMA}/versions/{VERSION}
    private static final String versionPath;
    //records/{ID}/representations/{SCHEMA}/versions/{VERSION}/copy
    private static final String copyPath;
    //records/{ID}/representations/{SCHEMA}/versions/{VERSION}/persist
    private static final String persistPath;

    static {
        StringBuilder builder = new StringBuilder();

        builder.append(ParamConstants.RECORDS);
        builder.append("/");
        builder.append("{");
        builder.append(ParamConstants.P_GID);
        builder.append("}");
        recordPath = builder.toString();

        builder.append("/");
        builder.append(ParamConstants.REPRESENTATIONS);
        representationsPath = builder.toString();

        builder.append("/");
        builder.append("{");
        builder.append(ParamConstants.P_SCHEMA);
        builder.append("}");
        schemaPath = builder.toString();

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
 
    }

    /**
     * Constructs a RecordServiceClient
     *
     * @param baseUrl URL of the MCS Rest Service
     */
    public RecordServiceClient(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    /**
     * Function returns record with all its latest persistent representation.
     *
     * @param cloudId id of getting Record
     * @return Record of specified cloudId
     * @throws RecordNotExistsException when id is not known UIS Service.
     * @throws MCSException on unexpected situations.
     */
    public Record getRecord(String cloudId)
            throws RecordNotExistsException, MCSException {
        WebTarget target = client.target(baseUrl).path(recordPath + "/").resolveTemplate("ID", cloudId);
        Builder request = target.request();
        Response response = request.get();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            return response.readEntity(Record.class);

        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }

    /**
     * Function deletes record from MSC Service with all its Representations in
     * all Versions. Mapping from UIS is not removed.
     *
     * @param cloudId id of deleting Record.
     * @return true if operation was successful
     * @throws RecordNotExistsException when id is not known UIS Service.
     * @throws MCSException on unexpected situations.
     */
    public boolean deleteRecord(String cloudId)
            throws RecordNotExistsException, MCSException {
        WebTarget target = client.target(baseUrl).path(recordPath + "/").resolveTemplate("ID", cloudId);
        Builder request = target.request();
        Response response = request.delete();
        if (response.getStatus() == Response.Status.NO_CONTENT.getStatusCode()) {
            return true;
        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }

    /**
     * Function returns list of all latest persistent versions of record
     * representations.
     *
     * @param cloudId id of record from get list of representations.
     * @return list of representations.
     * @throws RecordNotExistsException when id is not known UIS Service.
     * @throws MCSException on unexpected situations.
     */
    public List<Representation> getRepresentations(String cloudId)
            throws RecordNotExistsException, MCSException {
        WebTarget target = client.target(baseUrl).path(representationsPath + "/").resolveTemplate("ID", cloudId);
        Response response = target.request().get();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            List<Representation> entity = response.readEntity(new GenericType<List<Representation>>() {
            });
            return entity;
        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }

    /**
     * Function returns latest persistent version of representation.
     *
     * @param cloudId id of record from get representation.
     * @param schema schema from get representation.
     * @return representation of specified schema and cloudId.
     * @throws RecordNotExistsException representation does not exist or no
     * persistent version of this representation exists.
     * @throws MCSException on unexpected situations.
     */
    public Representation getRepresentation(String cloudId, String schema)
            throws RecordNotExistsException, MCSException {
        WebTarget target = client.target(baseUrl).path(schemaPath + "/")
                .resolveTemplate("ID", cloudId).resolveTemplate("SCHEMA", schema);
        Builder request = target.request();
        Response response = request.get();
        if (response.getStatus() == Response.Status.OK.getStatusCode()
                || response.getStatus() == Response.Status.TEMPORARY_REDIRECT.getStatusCode()) {
            Representation representation = response.readEntity(Representation.class);
            return representation;
        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }

    /**
     * Function creates new representation version.
     *
     * @param cloudId id of creating representation.
     * @param schema schema of creating representation.
     * @param providerId providerId of creating representation.
     * @return URI to created representation.
     * @throws ProviderNotExistsException when no provider with given id exist.
     * @throws RecordNotExistsException when id is not known UIS Service.
     * @throws MCSException on unexpected situations.
     */
    public URI createRepresentation(String cloudId, String schema, String providerId)
            throws ProviderNotExistsException, RecordNotExistsException, MCSException {
        WebTarget target = client.target(baseUrl).path(schemaPath + "/")
                .resolveTemplate("ID", cloudId).resolveTemplate("SCHEMA", schema);
        Builder request = target.request();
        Form form = new Form();
        form.param("providerId", providerId);
        Response response = request.post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));
        if (response.getStatus() == Response.Status.CREATED.getStatusCode()) {
            URI uri = response.getLocation();
            return uri;
        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }

    /**
     * Function deletes representation with all versions.
     *
     * @param cloudId id of deleting representation.
     * @param schema schema of deleting representation.
     * @return true if operation success.
     * @throws RepresentationNotExistsException if specified Representation does
     * not exist.
     * @throws MCSException on unexpected situations.
     */
    public boolean deletesRepresentation(String cloudId, String schema)
            throws RepresentationNotExistsException, MCSException {
        WebTarget target = client.target(baseUrl).path(schemaPath + "/")
                .resolveTemplate("ID", cloudId).resolveTemplate("SCHEMA", schema);
        Builder request = target.request();
        Response responseDelete = request.delete();
        if (responseDelete.getStatus() == Response.Status.NO_CONTENT.getStatusCode()) {
            return true;
        } else {
            ErrorInfo errorInfo = responseDelete.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }

    }

    /**
     * Function gets list all versions of record representation.
     *
     * @param cloudId id of getting representation list.
     * @param schema schema of getting representation list.
     * @return representation list.
     * @throws RepresentationNotExistsException if specified Representation does
     * not exist.
     * @throws MCSException on unexpected situations.
     */
    public List<Representation> getRepresentations(String cloudId, String schema)
            throws RepresentationNotExistsException, MCSException {
        WebTarget target = client.target(baseUrl).path(versionsPath + "/")
                .resolveTemplate("ID", cloudId).resolveTemplate("SCHEMA", schema);
        Builder request = target.request();
        Response response = request.get();
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            List<Representation> list = response.readEntity(new GenericType<List<Representation>>() {
            });
            return list;
        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }

    /**
     * Function returns representation in specified version.
     *
     * @param cloudId id of getting representation.
     * @param schema schema of getting representation.
     * @param version version of getting representation. If version = LATEST
     * function will return latest persistent version.
     * @return requested representation.
     * @throws RepresentationNotExistsException if specified representation does
     * not exist.
     * @throws MCSException on unexpected situations.
     */
    public Representation getRepresentation(String cloudId, String schema, String version)
            throws RepresentationNotExistsException, MCSException {
        WebTarget webtarget = client.target(baseUrl).path(versionPath + "/")
                .resolveTemplate("ID", cloudId).resolveTemplate("SCHEMA", schema).resolveTemplate("VERSION", version);
        Builder request = webtarget.request();
        Response response = request.get();
        System.out.println(response);
        if (response.getStatus() == Response.Status.OK.getStatusCode()) {
            Representation representation = response.readEntity(Representation.class);
            return representation;
        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }

    /**
     * Function deletes Representation in specified version.
     *
     * @param cloudId id of deleting Representation.
     * @param schema schema of deleting Representation.
     * @param version version of deleting Representation.
     * @return true if object was deleted.
     * @throws RepresentationNotExistsException if specified Representation does
     * not exist.
     * @throws CannotModifyPersistentRepresentationException if specified
     * Representation is persistent as such cannot be removed.
     * @throws MCSException on unexpected situations.
     */
    public boolean deleteRepresentation(String cloudId, String schema, String version)
            throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException, MCSException {
        WebTarget webtarget = client.target(baseUrl).path(versionPath + "/")
                .resolveTemplate("ID", cloudId).resolveTemplate("SCHEMA", schema).resolveTemplate("VERSION", version);
        Builder request = webtarget.request();
        Response responseDelete = request.delete();
        if (responseDelete.getStatus() == Response.Status.NO_CONTENT.getStatusCode()) {
            return true;
        } else {
            ErrorInfo errorInfo = responseDelete.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }

    }

    /**
     * Function Copies all information with all files and their content from one
     * representation version to a new temporary one.
     *
     * @param cloudId id of copying representation.
     * @param schema schema of copying representation.
     * @param version version of copying representation.
     * @return URI to created copy of representation.
     * @throws RepresentationNotExistsException if specified representation does
     * not exist.
     * @throws MCSException on unexpected situations.
     */
    public URI copyRepresentation(String cloudId, String schema, String version)
            throws RepresentationNotExistsException, MCSException {
        WebTarget target = client.target(baseUrl)
                .path(copyPath + "/").resolveTemplate("ID", cloudId)
                .resolveTemplate("SCHEMA", schema).resolveTemplate("VERSION", version);
        Builder request = target.request();
        Response response = request.post(Entity.entity(new Form(), MediaType.APPLICATION_FORM_URLENCODED_TYPE));
        if (response.getStatus() == Response.Status.CREATED.getStatusCode()) {
            return response.getLocation();
        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }

    /**
     * Function persist temporary representation.
     *
     * @param cloudId id of persisting representation.
     * @param schema schema of persisting representation.
     * @param version version of persisting representation.
     * @return URI to persisted representation.
     * @throws RepresentationNotExistsException when representation does not
     * exist in specified version.
     * @throws CannotModifyPersistentRepresentationException when representation
     * version is already persistent.
     * @throws CannotPersistEmptyRepresentationException when representation
     * version has no file attached and as such cannot be made persistent.
     * @throws MCSException on unexpected situations.
     */
    public URI persistRepresentation(String cloudId, String schema, String version)
            throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException,
            CannotPersistEmptyRepresentationException, MCSException {
        WebTarget target = client.target(baseUrl)
                .path(persistPath + "/")
                .resolveTemplate("ID", cloudId).resolveTemplate("SCHEMA", schema).resolveTemplate("VERSION", version);
        Form form = new Form();
        Builder request = target.request();
        Response response = request.post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE));
        if (response.getStatus() == Response.Status.CREATED.getStatusCode()) {
            URI uri = response.getLocation();
            return uri;
        } else {
            ErrorInfo errorInfo = response.readEntity(ErrorInfo.class);
            throw MCSExceptionProvider.generateException(errorInfo);
        }
    }

}
