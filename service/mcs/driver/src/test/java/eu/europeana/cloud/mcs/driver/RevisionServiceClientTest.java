package eu.europeana.cloud.mcs.driver;

import co.freeside.betamax.Betamax;
import co.freeside.betamax.Recorder;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.utils.Tags;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.junit.*;

import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RevisionServiceClientTest {

    @Rule
    public Recorder recorder = new Recorder();

    private static final String baseUrl = "http://localhost:8080/mcs/";
    private static final String CLOUD_ID = "test_cloud_id";
    private static final String REPRESENTATION_NAME = "test_representation";
    private static final String REVISION_NAME = "test_revision_name";
    private static final String PROVIDER_ID = "test_provider_id";
    private static final String VERSION = "de084210-a393-11e3-8614-50e549e85271";

    private static final String EXPECTED_REVISIONS_LOCATION = "http://localhost:8080/mcs/records/test_cloud_id/representations/test_representation/versions/de084210-a393-11e3-8614-50e549e85271/revisions";
    private static final String EXPECTED_REVISION_PATH_WITH_ACCEPTED_TAG = "http://localhost:8080/mcs/records/test_cloud_id/representations/test_representation/versions/de084210-a393-11e3-8614-50e549e85271/revisions/test_revision_name/revisionProvider/test_provider_id/tag/acceptance";
    private static final String EXPECTED_REVISION_PATH_WITH_PUBLISHED_TAG = "http://localhost:8080/mcs/records/test_cloud_id/representations/test_representation/versions/de084210-a393-11e3-8614-50e549e85271/revisions/test_revision_name/revisionProvider/test_provider_id/tag/published";
    private static final String EXPECTED_REVISION_PATH_WITH_DELETED_TAG = "http://localhost:8080/mcs/records/test_cloud_id/representations/test_representation/versions/de084210-a393-11e3-8614-50e549e85271/revisions/test_revision_name/revisionProvider/test_provider_id/tag/deleted";
    private static final String EXPECTED_REVISION_PATH_MULTIPLE_TAGS = "http://localhost:8080/mcs/records/test_cloud_id/representations/test_representation/versions/de084210-a393-11e3-8614-50e549e85271/revisions/test_revision_name/revisionProvider/test_provider_id/tags";
    private RevisionServiceClient instance;

    @Before
    public void setUp() {
        instance = new RevisionServiceClient(baseUrl);
    }

    @Betamax(tape = "revisions/shouldAddRevision")
    @org.junit.Test
    public void shouldSuccessfullyAddRevision()
            throws MCSException, IOException {
        Revision revision = new Revision(REVISION_NAME, PROVIDER_ID);
        URI uri = instance.addRevision(CLOUD_ID, REPRESENTATION_NAME, VERSION, revision);
        assertNotNull(uri);
        assertEquals(EXPECTED_REVISIONS_LOCATION, uri.toString());
    }

    @Betamax(tape = "revisions/shouldAddRevisionWithAcceptanceTag")
    @org.junit.Test
    public void shouldAddRevisionWithAcceptanceTag()
            throws MCSException, IOException {
        URI uri = instance.addRevision(CLOUD_ID, REPRESENTATION_NAME, VERSION, REVISION_NAME, PROVIDER_ID, Tags.ACCEPTANCE.getTag());
        assertNotNull(uri);
        assertEquals(uri.toString(), EXPECTED_REVISION_PATH_WITH_ACCEPTED_TAG);


    }

    @Betamax(tape = "revisions/shouldAddRevisionWithPublishedTag")
    @org.junit.Test
    public void shouldAddRevisionWithPublishedTag()
            throws MCSException, IOException {
        URI uri = instance.addRevision(CLOUD_ID, REPRESENTATION_NAME, VERSION, REVISION_NAME, PROVIDER_ID, Tags.PUBLISHED.getTag());
        assertNotNull(uri);
        assertEquals(uri.toString(), EXPECTED_REVISION_PATH_WITH_PUBLISHED_TAG);
    }

    @Betamax(tape = "revisions/shouldAddRevisionWithDeletedTag")
    @org.junit.Test
    public void shouldAddRevisionWithDeletedTag()
            throws MCSException, IOException {
        URI uri = instance.addRevision(CLOUD_ID, REPRESENTATION_NAME, VERSION, REVISION_NAME, PROVIDER_ID, Tags.DELETED.getTag());
        assertNotNull(uri);
        assertEquals(uri.toString(), EXPECTED_REVISION_PATH_WITH_DELETED_TAG);

    }


    @Betamax(tape = "revisions/shouldAddRevisionWithMultipleTags")
    @org.junit.Test
    public void shouldAddRevisionWithMultipleTags()
            throws MCSException, IOException {
        Set<Tags> tags = new HashSet<>();
        tags.add(Tags.ACCEPTANCE);
        tags.add(Tags.DELETED);
        URI uri = instance.addRevision(CLOUD_ID, REPRESENTATION_NAME, VERSION, REVISION_NAME, PROVIDER_ID, tags);
        assertNotNull(uri);
        assertEquals(uri.toString(), EXPECTED_REVISION_PATH_MULTIPLE_TAGS);

    }


    @Betamax(tape = "revisions/shouldAddRevisionWithMultipleTags")
    @org.junit.Test
    public void shouldAddRevisionWithEmptyTagsSet()
            throws MCSException, IOException {
        Set<Tags> tags = new HashSet<>();
        URI uri = instance.addRevision(CLOUD_ID, REPRESENTATION_NAME, VERSION, REVISION_NAME, PROVIDER_ID, tags);
        assertNotNull(uri);
        assertEquals(uri.toString(), EXPECTED_REVISION_PATH_MULTIPLE_TAGS);

    }
}