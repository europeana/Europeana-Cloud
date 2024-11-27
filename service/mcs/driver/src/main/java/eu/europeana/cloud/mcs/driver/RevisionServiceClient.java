package eu.europeana.cloud.mcs.driver;

import static eu.europeana.cloud.common.log.AttributePassingUtils.passLogContext;
import static eu.europeana.cloud.common.web.ParamConstants.CLOUD_ID;
import static eu.europeana.cloud.common.web.ParamConstants.F_REVISION_TIMESTAMP;
import static eu.europeana.cloud.common.web.ParamConstants.REPRESENTATION_NAME;
import static eu.europeana.cloud.common.web.ParamConstants.REVISION_NAME;
import static eu.europeana.cloud.common.web.ParamConstants.REVISION_PROVIDER_ID;
import static eu.europeana.cloud.common.web.ParamConstants.TAG;
import static eu.europeana.cloud.common.web.ParamConstants.VERSION;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REVISION_ADD;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REVISION_ADD_WITH_PROVIDER_TAG;
import static eu.europeana.cloud.service.mcs.RestInterfaceConstants.REVISION_DELETE;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.mcs.driver.exception.DriverException;
import eu.europeana.cloud.service.commons.utils.DateHelper;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import java.net.URI;

import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

/**
 * Created by Tarek on 8/2/2016.
 */
public class RevisionServiceClient extends MCSClient {

  /**
   * Constructs a RevisionServiceClient
   *
   * @param baseUrl url of the MCS Rest Service
   */

  public RevisionServiceClient(String baseUrl) {
    this(baseUrl, DEFAULT_CONNECT_TIMEOUT_IN_MILLIS, DEFAULT_READ_TIMEOUT_IN_MILLIS);
  }

  public RevisionServiceClient(String baseUrl, final int connectTimeoutInMillis, final int readTimeoutInMillis) {
    super(baseUrl);

    this.client.property(ClientProperties.CONNECT_TIMEOUT, connectTimeoutInMillis);
    this.client.property(ClientProperties.READ_TIMEOUT, readTimeoutInMillis);
  }

  /**
   * Creates instance of RevisionServiceClient. Same as {@link #RevisionServiceClient(String)} but includes username and password
   * to perform authenticated requests.
   *
   * @param baseUrl URL of the MCS Rest Service
   */

  public RevisionServiceClient(String baseUrl, final String username, final String password) {
    this(baseUrl, username, password, DEFAULT_CONNECT_TIMEOUT_IN_MILLIS, DEFAULT_READ_TIMEOUT_IN_MILLIS);
  }


  public RevisionServiceClient(String baseUrl, final String username, final String password, final int connectTimeoutInMillis,
      final int readTimeoutInMillis) {
    super(baseUrl);

    client.register(HttpAuthenticationFeature.basicBuilder().credentials(username, password).build());
    client.property(ClientProperties.CONNECT_TIMEOUT, connectTimeoutInMillis);
    client.property(ClientProperties.READ_TIMEOUT, readTimeoutInMillis);
  }

  /**
   * add a revision
   *
   * @param cloudId id of uploaded revision.
   * @param representationName representation name of uploaded revision.
   * @param version a specific version of the representation.
   * @param revisionName the name of revision
   * @param revisionProviderId revision provider id
   * @param tag flag tag
   * @return URI to specific revision with specific tag inside a version.
   * @throws DriverException call to service has not succeeded because of server side error.
   * @throws MCSException on unexpected situations.
   */
  public URI addRevision(String cloudId, String representationName, String version, String revisionName,
      String revisionProviderId, String tag) throws MCSException {
    return manageResponse(new ResponseParams<>(URI.class, Response.Status.CREATED),
        () -> passLogContext(client
            .target(baseUrl)
            .path(REVISION_ADD_WITH_PROVIDER_TAG)
            .resolveTemplate(CLOUD_ID, cloudId)
            .resolveTemplate(REPRESENTATION_NAME, representationName)
            .resolveTemplate(VERSION, version)
            .resolveTemplate(REVISION_NAME, revisionName)
            .resolveTemplate(REVISION_PROVIDER_ID, revisionProviderId)
            .resolveTemplate(TAG, tag)
            .request())
            .post(null)
    );
  }

  /**
   * add a revision
   *
   * @param cloudId id of uploaded revision.
   * @param representationName representation name of uploaded revision.
   * @param version a specific version of the representation.
   * @param revision revision
   * @return URI to revisions inside a version.
   * @throws RepresentationNotExistsException when representation does not exist in specified version.
   * @throws DriverException call to service has not succeeded because of server side error.
   * @throws MCSException on unexpected situations.
   */
  public URI addRevision(String cloudId, String representationName, String version, Revision revision) throws MCSException {
    return manageResponse(new ResponseParams<>(URI.class, Response.Status.CREATED),
        () -> passLogContext(client
            .target(baseUrl)
            .path(REVISION_ADD)
            .resolveTemplate(CLOUD_ID, cloudId)
            .resolveTemplate(REPRESENTATION_NAME, representationName)
            .resolveTemplate(VERSION, version)
            .request())
            .accept(MediaType.APPLICATION_JSON).post(Entity.json(revision))
    );
  }

  /**
   * Remove a revision
   *
   * @param cloudId cloud Id
   * @param representationName representation name
   * @param version representation version
   * @param revision the revision to delete
   * @throws RepresentationNotExistsException throws if given representation not exists
   */
  public void deleteRevision(String cloudId, String representationName, String version, Revision revision) throws MCSException {
    manageResponse(new ResponseParams<>(Void.class, Response.Status.NO_CONTENT),
        () -> passLogContext(client
            .target(baseUrl)
            .path(REVISION_DELETE)
            .resolveTemplate(CLOUD_ID, cloudId)
            .resolveTemplate(REPRESENTATION_NAME, representationName)
            .resolveTemplate(VERSION, version)
            .resolveTemplate(REVISION_NAME, revision.getRevisionName())
            .resolveTemplate(REVISION_PROVIDER_ID, revision.getRevisionProviderId())
            .queryParam(F_REVISION_TIMESTAMP, DateHelper.getISODateString(revision.getCreationTimeStamp()))
            .request())
            .delete()
    );
  }
}
