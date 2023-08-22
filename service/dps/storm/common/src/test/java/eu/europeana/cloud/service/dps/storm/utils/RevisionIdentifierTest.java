package eu.europeana.cloud.service.dps.storm.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import eu.europeana.cloud.common.model.Revision;
import java.util.Date;
import org.junit.Test;

public class RevisionIdentifierTest {

  private static final String REVISION_NAME_1 = "revisionName1";
  private static final String REVISION_NAME_2 = "revisionName2";

  private static final String REVISION_PROVIDER_1 = "revisionName1";
  private static final String REVISION_PROVIDER_2 = "revisionName2";

  private static final Date REVISION_TIMESTAMP_1 = new Date(0);
  private static final Date REVISION_TIMESTAMP_2 = new Date(1);

  @Test
  public void identifiesShouldReturnTrueWhileRevisionNameProviderAndTimestampAreTheSameAsInRevision() {
    RevisionIdentifier id = new RevisionIdentifier(REVISION_NAME_1, REVISION_PROVIDER_1, REVISION_TIMESTAMP_1);
    Revision revision = new Revision(REVISION_NAME_1, REVISION_PROVIDER_1, REVISION_TIMESTAMP_1, false);

    assertTrue(id.identifies(revision));
  }

  @Test
  public void identifiesShouldReturnFalseWhileRevisionNameIsDifferentInRevision() {
    RevisionIdentifier id = new RevisionIdentifier(REVISION_NAME_1, REVISION_PROVIDER_1, REVISION_TIMESTAMP_1);
    Revision revision = new Revision(REVISION_NAME_2, REVISION_PROVIDER_1, REVISION_TIMESTAMP_1, false);

    assertFalse(id.identifies(revision));
  }

  @Test
  public void identifiesShouldReturnFalseWhileRevisionProviderIsDifferentInRevision() {
    RevisionIdentifier id = new RevisionIdentifier(REVISION_NAME_1, REVISION_PROVIDER_1, REVISION_TIMESTAMP_1);
    Revision revision = new Revision(REVISION_NAME_1, REVISION_PROVIDER_2, REVISION_TIMESTAMP_1, false);

    assertFalse(id.identifies(revision));
  }

  @Test
  public void identifiesShouldReturnFalseWhileTimestampIsDifferentInRevision() {
    RevisionIdentifier id = new RevisionIdentifier(REVISION_NAME_1, REVISION_PROVIDER_1, REVISION_TIMESTAMP_1);
    Revision revision = new Revision(REVISION_NAME_1, REVISION_PROVIDER_2, REVISION_TIMESTAMP_2, false);

    assertFalse(id.identifies(revision));
  }

  @Test
  public void identifiesShouldReturnTrueWhileIdentifierHasNotTimestampButRevisionNameProviderAreTheSameAsInRevision() {
    RevisionIdentifier id = new RevisionIdentifier(REVISION_NAME_1, REVISION_PROVIDER_1, null);
    Revision revision = new Revision(REVISION_NAME_1, REVISION_PROVIDER_1, REVISION_TIMESTAMP_1, false);

    assertTrue(id.identifies(revision));
  }

}