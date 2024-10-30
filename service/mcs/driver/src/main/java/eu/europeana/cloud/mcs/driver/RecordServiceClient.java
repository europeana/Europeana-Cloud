package eu.europeana.cloud.mcs.driver;

import static eu.europeana.cloud.common.log.AttributePassingUtils.passLogContext;
import static eu.europeana.cloud.common.web.ParamConstants.CLOUD_ID;
import static eu.europeana.cloud.common.web.ParamConstants.DATA_SET_ID;
import static eu.europeana.cloud.common.web.ParamConstants.F_DATASET;
import static eu.europeana.cloud.common.web.ParamConstants.F_REVISION_PROVIDER_ID;
import static eu.europeana.cloud.common.web.ParamConstants.F_REVISION_TIMESTAMP;
import static eu.europeana.cloud.common.web.ParamConstants.PROVIDER_ID;
import static eu.europeana.cloud.common.web.ParamConstants.REPRESENTATION_NAME;
import static eu.europeana.cloud.common.web.ParamConstants.REVISION_NAME;
import static eu.europeana.cloud.common.web.ParamConstants.VERSION;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.FILE_UPLOAD_RESOURCE;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.RECORDS_RESOURCE;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATIONS_RESOURCE;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATION_RAW_REVISIONS_RESOURCE;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATION_RESOURCE;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATION_REVISIONS_RESOURCE;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATION_VERSION;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATION_VERSIONS_RESOURCE;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REPRESENTATION_VERSION_PERSIST;

import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.CannotPersistEmptyRepresentationException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.UUID;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Form;
import jakarta.ws.rs.core.GenericType;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.commons.io.IOUtils;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;

/**
 * Exposes API related for records.
 */
public class RecordServiceClient extends MCSClient {

  /**
   * Creates instance of RecordServiceClient.
   *
   * @param baseUrl URL of the MCS Rest Service
   */
  public RecordServiceClient(String baseUrl) {
    this(baseUrl, DEFAULT_CONNECT_TIMEOUT_IN_MILLIS, DEFAULT_READ_TIMEOUT_IN_MILLIS);
  }

  public RecordServiceClient(String baseUrl, final int connectTimeoutInMillis, final int readTimeoutInMillis) {
    this(baseUrl, null, null, connectTimeoutInMillis, readTimeoutInMillis);
  }

  /**
   * Creates instance of RecordServiceClient. Same as {@link #RecordServiceClient(String)} but includes username and password to
   * perform authenticated requests.
   *
   * @param baseUrl URL of the MCS Rest Service
   */
  public RecordServiceClient(String baseUrl, final String username, final String password) {
    this(baseUrl, username, password, DEFAULT_CONNECT_TIMEOUT_IN_MILLIS, DEFAULT_READ_TIMEOUT_IN_MILLIS);
  }

  /**
   * All parameters' constructor used by another one
   *
   * @param baseUrl URL of the MCS Rest Service
   * @param username Username to HTTP authorisation  (use together with password)
   * @param password Password to HTTP authorisation (use together with username)
   * @param connectTimeoutInMillis Timeout for waiting for connecting
   * @param readTimeoutInMillis Timeout for getting data
   */
  public RecordServiceClient(String baseUrl, final String username, final String password,
      final int connectTimeoutInMillis, final int readTimeoutInMillis) {
    super(baseUrl);

    if (username != null || password != null) {
      client.register(HttpAuthenticationFeature.basicBuilder().credentials(username, password).build());
    }

    this.client.property(ClientProperties.CONNECT_TIMEOUT, connectTimeoutInMillis);
    this.client.property(ClientProperties.READ_TIMEOUT, readTimeoutInMillis);
  }

  public static void main(String[] args) {
    RecordServiceClient c = new RecordServiceClient("http://127.0.0.1:8080/mcs");
      try {
          c.getRecord("sample");
      } catch (MCSException e) {
          throw new RuntimeException(e);
      }

  }
  /**
   * Returns record with all its latest persistent representations.
   *
   * @param cloudId id of the record (required)
   * @return record of specified cloudId (required)
   * @throws RecordNotExistsException when id is not known UIS Service
   * @throws MCSException on unexpected situations
   */
  public Record getRecord(String cloudId) throws MCSException {
    return manageResponse(new ResponseParams<>(Record.class),
        () -> passLogContext(client
            .target(baseUrl)
            .path(RECORDS_RESOURCE)
            .resolveTemplate(CLOUD_ID, cloudId)
            .request())
            .get()
    );
  }

  /**
   * Deletes record with all its representations in all versions.
   * <p/>
   * Does not remove mapping from Unique Identifier Service. If record exists, but nothing was deleted (it had no representations
   * assigned), nothing happens.
   *
   * @param cloudId id of deleted record (required)
   * @throws RecordNotExistsException if cloudId is not known UIS Service
   * @throws MCSException on unexpected situations
   */
  public void deleteRecord(String cloudId) throws MCSException {
    manageResponse(new ResponseParams<>(Void.class, Response.Status.NO_CONTENT),
        () -> passLogContext(client
            .target(baseUrl)
            .path(RECORDS_RESOURCE)
            .resolveTemplate(CLOUD_ID, cloudId)
            .request())
            .delete()
    );
  }

  /**
   * Lists all latest persistent versions of record representation.
   *
   * @param cloudId id of record from which to get representations (required)
   * @return list of representations
   * @throws RecordNotExistsException if cloudId is not known UIS Service
   * @throws MCSException on unexpected situations
   */
  public List<Representation> getRepresentations(String cloudId) throws MCSException {
    return manageResponse(new ResponseParams<>(new GenericType<List<Representation>>() {
        }),
        () -> passLogContext(client
            .target(baseUrl)
            .path(REPRESENTATIONS_RESOURCE)
            .resolveTemplate(CLOUD_ID, cloudId)
            .request())
            .get()
    );
  }

  /**
   * Returns latest persistent version of representation.
   *
   * @param cloudId id of record from which to get representations (required)
   * @param representationName name of the representation (required)
   * @return representation of specified representationName and cloudId
   * @throws RepresentationNotExistsException representation does not exist or no persistent version of this representation
   * exists
   * @throws MCSException on unexpected situations
   */
  public Representation getRepresentation(String cloudId, String representationName) throws MCSException {
    return manageResponse(
        new ResponseParams<>(Representation.class, new Response.Status[]{Response.Status.OK, Response.Status.TEMPORARY_REDIRECT}),
        () -> passLogContext(client
            .target(baseUrl)
            .path(REPRESENTATION_RESOURCE)
            .resolveTemplate(CLOUD_ID, cloudId)
            .resolveTemplate(REPRESENTATION_NAME, representationName)
            .request())
            .get()
    );
  }

  /**
   * Creates new representation version.
   *
   * @param cloudId id of the record in which to create the representation (required)
   * @param representationName name of the representation to be created (required)
   * @param providerId provider of this representation version (required)
   * @param version representation's version
   * @return URI to the created representation
   * @throws ProviderNotExistsException when no provider with given id exists
   * @throws RecordNotExistsException when cloud id is not known to UIS Service
   * @throws MCSException on unexpected situations
   */
  public URI createRepresentation(String cloudId, String representationName, String providerId, UUID version,
      String datasetId) throws MCSException {
    var form = new Form();
    form.param(PROVIDER_ID, providerId);
    form.param(DATA_SET_ID, datasetId);
    if (version != null) {
      form.param(VERSION, version.toString());
    }
    return manageResponse(new ResponseParams<>(URI.class, Response.Status.CREATED),
        () -> passLogContext(client.target(baseUrl)
                    .path(REPRESENTATION_RESOURCE)
                    .resolveTemplate(CLOUD_ID, cloudId)
                    .resolveTemplate(REPRESENTATION_NAME, representationName)
                    .request())
                    .post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED_TYPE))
    );
  }

  public URI createRepresentation(String cloudId, String representationName, String providerId, String datasetId)
      throws MCSException {
    return createRepresentation(cloudId, representationName, providerId, null, datasetId);
  }

  /**
   * Creates new representation version, uploads a file and makes this representation persistent (in one request)
   *
   * @param cloudId id of the record in which to create the representation (required)
   * @param representationName name of the representation to be created (required)
   * @param providerId provider of this representation version (required)
   * @param data file that should be uploaded (required)
   * @param fileName name for created file
   * @param mediaType mimeType of uploaded file
   * @return URI to created file
   */
  public URI createRepresentation(String cloudId, String representationName, String providerId, String datasetId,
      InputStream data, String fileName, String mediaType) throws IOException, MCSException {

    var multiPart = prepareRequestBody(providerId, datasetId, data, fileName, mediaType);
    try {
      return manageResponse(new ResponseParams<>(URI.class, Response.Status.CREATED),
          () -> passLogContext(client
              .target(baseUrl)
              .path(FILE_UPLOAD_RESOURCE)
              .resolveTemplate(CLOUD_ID, cloudId)
              .resolveTemplate(REPRESENTATION_NAME, representationName)
              .request())
              .header("Content-Type", "multipart/form-data")
              .post(Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA)));
    } finally {
      closeDataSources(data, multiPart);
    }
  }

  /**
   * Creates new representation version, uploads a file and makes this representation persistent (in one request)
   *
   * @param cloudId id of the record in which to create the representation (required)
   * @param representationName name of the representation to be created (required)
   * @param providerId provider of this representation version (required)
   * @param data file that should be uploaded (required)
   * @param fileName name for created file
   * @param mediaType mimeType of uploaded file
   * @return URI to created file
   */
  public URI createRepresentation(String cloudId, String representationName, String providerId, UUID version,
      String datasetId, InputStream data, String fileName, String mediaType) throws IOException, MCSException {

    var multiPart = prepareRequestBody(providerId, datasetId, data, fileName, mediaType);
    if (version != null) {
      multiPart.field(VERSION, version.toString());
    }

    try {
      return manageResponse(new ResponseParams<>(URI.class, Response.Status.CREATED),
          () -> passLogContext(client
              .target(baseUrl)
              .path(FILE_UPLOAD_RESOURCE)
              .resolveTemplate(CLOUD_ID, cloudId)
              .resolveTemplate(REPRESENTATION_NAME, representationName)
              .request())
              .post(Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA))
      );
    } finally {
      closeDataSources(data, multiPart);
    }
  }


  /**
   * Creates new representation version, uploads a file and makes this representation persistent (in one request)
   *
   * @param cloudId id of the record in which to create the representation (required)
   * @param representationName name of the representation to be created (required)
   * @param providerId provider of this representation version (required)
   * @param data file that should be uploaded (required)
   * @param mediaType mimeType of uploaded file
   * @return URI to created file
   * @throws MCSException throws if something went wrong
   */
  public URI createRepresentation(String cloudId, String representationName, String providerId, String datasetId,
      InputStream data, String mediaType) throws IOException, MCSException {

    return this.createRepresentation(cloudId, representationName, providerId, datasetId, data, null, mediaType);
  }

  /**
   * Deletes representation with all versions.
   *
   * @param cloudId id of the record to delete representation from (required)
   * @param representationName representation name of deleted representation (required)
   * @throws RepresentationNotExistsException if specified Representation does not exist
   * @throws MCSException on unexpected situations
   */
  public void deleteRepresentation(String cloudId, String representationName) throws MCSException {

    manageResponse(new ResponseParams<>(Void.class, Response.Status.NO_CONTENT),
        () -> passLogContext(client
            .target(baseUrl)
            .path(REPRESENTATION_RESOURCE)
            .resolveTemplate(CLOUD_ID, cloudId)
            .resolveTemplate(REPRESENTATION_NAME, representationName)
            .request())
            .delete()
    );
  }

  /**
   * Lists all versions of record representation.
   *
   * @param cloudId id of the record to get representation from (required)
   * @param representationName name of the representation (required)
   * @return representation versions list
   * @throws RepresentationNotExistsException if specified Representation does not exist
   * @throws MCSException on unexpected situations
   */
  public List<Representation> getRepresentations(String cloudId, String representationName) throws MCSException {

    return manageResponse(new ResponseParams<>(new GenericType<List<Representation>>() {
        }),
        () -> passLogContext(client
            .target(baseUrl)
            .path(REPRESENTATION_VERSIONS_RESOURCE)
            .resolveTemplate(CLOUD_ID, cloudId)
            .resolveTemplate(REPRESENTATION_NAME, representationName)
            .request())
            .get()
    );
  }

  /**
   * Returns representation in specified version.
   * <p/>
   * If Version = LATEST, will redirect to actual latest persistent version at the moment of invoking this method.
   *
   * @param cloudId id of the record to get representation from (required)
   * @param representationName name of the representation (required)
   * @param version version of the representation to be obtained; if version is equal LATEST function will return the latest
   * persistent version (required)
   * @return requested representation version
   * @throws RepresentationNotExistsException if specified representation does not exist
   * @throws MCSException on unexpected situations
   */
  public Representation getRepresentation(String cloudId, String representationName, String version) throws MCSException {
    return manageResponse(new ResponseParams<>(Representation.class),
        () -> passLogContext(client
            .target(baseUrl)
            .path(REPRESENTATION_VERSION)
            .resolveTemplate(CLOUD_ID, cloudId)
            .resolveTemplate(REPRESENTATION_NAME, representationName)
            .resolveTemplate(VERSION, version)
            .request())
            .get()
    );
  }

  /**
   * Returns representation in specified version.
   * <p/>
   * If Version = LATEST, will redirect to actual latest persistent version at the moment of invoking this method.
   *
   * @param cloudId id of the record to get representation from (required)
   * @param representationName name of the representation (required)
   * @param version version of the representation to be obtained; if version is equal LATEST function will return the latest
   * persistent version (required)
   * @param key key to header
   * @param value value to header
   * @return requested representation version
   * @throws RepresentationNotExistsException if specified representation does not exist
   * @throws MCSException on unexpected situations
   */
  public Representation getRepresentation(String cloudId, String representationName, String version, String key, String value)
      throws MCSException {
    return manageResponse(new ResponseParams<>(Representation.class),
        () -> passLogContext(client
            .target(baseUrl)
            .path(REPRESENTATION_VERSION)
            .resolveTemplate(CLOUD_ID, cloudId)
            .resolveTemplate(REPRESENTATION_NAME, representationName)
            .resolveTemplate(VERSION, version)
            .request())
            .header(key, value)
            .get()
    );
  }

  /**
   * Deletes representation in specified version.
   *
   * @param cloudId id of the record to delete representation version from (required)
   * @param representationName name of the representation (required)
   * @param version the deleted version of the representation (required)
   * @throws RepresentationNotExistsException if specified representation does not exist
   * @throws CannotModifyPersistentRepresentationException if specified representation is persistent and thus cannot be removed
   * @throws MCSException on unexpected situations
   */
  public void deleteRepresentation(String cloudId, String representationName, String version) throws MCSException {
    manageResponse(new ResponseParams<>(Void.class, Response.Status.NO_CONTENT),
        () -> passLogContext(client
            .target(baseUrl)
            .path(REPRESENTATION_VERSION)
            .resolveTemplate(CLOUD_ID, cloudId)
            .resolveTemplate(REPRESENTATION_NAME, representationName)
            .resolveTemplate(VERSION, version)
            .request())
            .delete()
    );
  }

  /**
   * Makes specified temporary representation version persistent.
   *
   * @param cloudId id of the record that holds representation (required)
   * @param representationName name of the representation to be persisted (required)
   * @param version version that should be made persistent (required)
   * @return URI to the persisted representation
   * @throws RepresentationNotExistsException when representation does not exist in specified version
   * @throws CannotModifyPersistentRepresentationException when representation version is already persistent
   * @throws CannotPersistEmptyRepresentationException when representation version has no file attached and thus cannot be made
   * persistent
   * @throws MCSException on unexpected situations
   */
  public URI persistRepresentation(String cloudId, String representationName, String version) throws MCSException {
    return manageResponse(new ResponseParams<>(URI.class, Response.Status.CREATED),
        () -> passLogContext(client
            .target(baseUrl)
            .path(REPRESENTATION_VERSION_PERSIST)
            .resolveTemplate(CLOUD_ID, cloudId)
            .resolveTemplate(REPRESENTATION_NAME, representationName)
            .resolveTemplate(VERSION, version)
            .request())
            .post(Entity.entity(new Form(), MediaType.APPLICATION_FORM_URLENCODED_TYPE))
    );
  }

  /**
   * Returns representation in specified version.
   * <p/>
   * If Version = LATEST, will redirect to actual latest persistent version at the moment of invoking this method.
   *
   * @param cloudId id of the record to get representation from (required)
   * @param representationName name of the representation (required)
   * @param revisionName revision name (required)
   * @param revisionProviderId revision provider identifier, together with revisionId it is used to determine the correct revision
   * (required)
   * @return requested representation version
   * @throws RepresentationNotExistsException if specified representation does not exist
   * @throws RepresentationNotExistsException on representation does not exist
   * @throws MCSException on unexpected situations
   * @deprecated since 6-SNAPSHOT. The method {@link #getRepresentationsByRevision(String, String, Revision)} should be used
   * instead
   */
  @Deprecated(since = "6-SNAPSHOT")
  public List<Representation> getRepresentationsByRevision(String cloudId, String representationName, String revisionName,
      String revisionProviderId, String revisionTimestamp) throws MCSException {
    return getRepresentationsByRevision(cloudId, representationName, new Revision(revisionName, revisionProviderId,
        DateHelper.parseISODate(revisionTimestamp)));
  }


  /**
   * Returns representation in specified version.
   * <p/>
   * If Version = LATEST, will redirect to actual latest persistent version at the moment of invoking this method.
   *
   * @param cloudId id of the record to get representation from (required)
   * @param representationName name of the representation (required)
   * @param revision the revision (required) (revisionProviderId is required)
   * @return requested representation version
   * @throws RepresentationNotExistsException if specified representation does not exist
   * @throws RepresentationNotExistsException on representation does not exist
   * @throws MCSException on unexpected situations
   */
  public List<Representation> getRepresentationsByRevision(String cloudId, String representationName, Revision revision)
      throws MCSException {

    if (revision.getRevisionProviderId() == null) {
      throw new MCSException("RevisionProviderId is required");
    }

    return manageResponse(new ResponseParams<>(new GenericType<List<Representation>>() {
        }),
        () -> passLogContext(client
            .target(baseUrl)
            .path(REPRESENTATION_REVISIONS_RESOURCE)
            .resolveTemplate(CLOUD_ID, cloudId)
            .resolveTemplate(REPRESENTATION_NAME, representationName)
            .resolveTemplate(REVISION_NAME, revision.getRevisionName())
            .queryParam(F_REVISION_PROVIDER_ID, revision.getRevisionProviderId())
            .queryParam(F_REVISION_TIMESTAMP, DateHelper.getISODateString(revision.getCreationTimeStamp()))
            .request())
            .get()
    );
  }

  /**
   * Returns raw revisions for the specified representation
   * <p/>
   * If Version = LATEST, will redirect to actual latest persistent version at the moment of invoking this method.
   *
   * @param cloudId id of the record to get representation from (required)
   * @param representationName name of the representation (required)
   * @param revision the revision (required) (revisionProviderId is required)
   * @return requested representation raw revisions
   * @throws RepresentationNotExistsException if specified representation does not exist
   * @throws MCSException on unexpected situations
   */
  public List<RepresentationRevisionResponse> getRepresentationRawRevisions(String cloudId, String representationName, Revision revision)
      throws MCSException {

    if (revision.getRevisionProviderId() == null) {
      throw new MCSException("RevisionProviderId is required");
    }

    return manageResponse(new ResponseParams<>(new GenericType<>() {
        }),
        () -> passLogContext(client
            .target(baseUrl)
            .path(REPRESENTATION_RAW_REVISIONS_RESOURCE)
            .resolveTemplate(CLOUD_ID, cloudId)
            .resolveTemplate(REPRESENTATION_NAME, representationName)
            .resolveTemplate(REVISION_NAME, revision.getRevisionName())
            .queryParam(F_REVISION_PROVIDER_ID, revision.getRevisionProviderId())
            .queryParam(F_REVISION_TIMESTAMP, DateHelper.getISODateString(revision.getCreationTimeStamp()))
            .request())
            .get()
    );
  }

  private FormDataMultiPart prepareRequestBody(String providerId, String datasetId,
      InputStream data, String fileName, String mediaType) {
    FormDataMultiPart requestBody = new FormDataMultiPart();
    requestBody
        .field(ParamConstants.F_PROVIDER, providerId)
        .field(F_DATASET, datasetId)
        .field(ParamConstants.F_FILE_MIME, mediaType)
        .bodyPart(new StreamDataBodyPart(ParamConstants.F_FILE_DATA, data, MediaType.APPLICATION_OCTET_STREAM));

    if (fileName == null || fileName.trim().isEmpty()) {
      fileName = UUID.randomUUID().toString();
    }
    requestBody.field(ParamConstants.F_FILE_NAME, fileName);

    return requestBody;
  }

  private void closeDataSources(InputStream data, MultiPart multiPartData) throws IOException {
    IOUtils.closeQuietly(data);
    if (multiPartData != null) {
      multiPartData.close();
    }
  }
}
