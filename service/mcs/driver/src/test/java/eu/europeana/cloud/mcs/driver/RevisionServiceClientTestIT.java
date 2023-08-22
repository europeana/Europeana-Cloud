package eu.europeana.cloud.mcs.driver;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.GregorianCalendar;
import org.junit.Test;

public class RevisionServiceClientTestIT {

  private static final String LOCAL_TEST_URL = "http://localhost:8080/mcs";

  private static final String USER_NAME = "metis_test";  //user z bazy danych
  private static final String USER_PASSWORD = "1RkZBuVf";

  @Test
  public void addRevision() throws MCSException {
    String cloudId = "enter_cloud_id_here";
    String representationName = "enter_representation_name_here";
    String version = "enter_version_here";

    String revisionName = "<enter_revision_name_here>";
    String revisionProviderId = "<enter_revision_provider_id_here>";
    Date creationTimeStamp = GregorianCalendar.getInstance().getTime();
    boolean deleted = false;
    Revision revision = new Revision(
        revisionName,
        revisionProviderId,
        creationTimeStamp,
        deleted
    );

    RevisionServiceClient mcsClient = new RevisionServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
    URI revisionUri = mcsClient.addRevision(cloudId, representationName, version, revision);

    assertNotNull(revisionUri);
  }

  @Test
  public void deleteRevison() throws MCSException {
    String cloudId = "<enter_cloud_id_here>";
    String representationName = "<enter_representation_name_here>";
    String version = "<enter_version_here>";
    String revisionName = "<enter_revision_name_here>";
    String revisionProviderId = "<enter_revision_provider_id_here>";
    String timestamp = "<enter_timestamp_here>";

    RevisionServiceClient mcsClient = new RevisionServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
    mcsClient.deleteRevision(cloudId, representationName, version,
        new Revision(revisionName, revisionProviderId, Date.from(ZonedDateTime.parse(timestamp).toInstant())));

    assertTrue(true);
  }

}
