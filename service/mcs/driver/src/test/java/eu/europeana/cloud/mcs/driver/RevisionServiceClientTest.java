package eu.europeana.cloud.mcs.driver;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.utils.Tags;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.test.WiremockHelper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static org.junit.Assert.*;

public class RevisionServiceClientTest {

    @Rule
    public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(8080));

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

    @Test
    public void shouldSuccessfullyAddRevision() throws MCSException {
        //
        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/test_cloud_id/representations/test_representation/versions/de084210-a393-11e3-8614-50e549e85271/revisions",
                201,
                EXPECTED_REVISIONS_LOCATION,
                null);
        //
        Revision revision = new Revision(REVISION_NAME, PROVIDER_ID);
        URI uri = instance.addRevision(CLOUD_ID, REPRESENTATION_NAME, VERSION, revision);
        assertNotNull(uri);
        assertEquals(EXPECTED_REVISIONS_LOCATION, uri.toString());
    }

    @Test
    public void shouldAddRevisionWithAcceptanceTag() throws MCSException {
        //
        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/test_cloud_id/representations/test_representation/versions/de084210-a393-11e3-8614-50e549e85271/revisions/test_revision_name/revisionProvider/test_provider_id/tag/acceptance",
                201,
                EXPECTED_REVISION_PATH_WITH_ACCEPTED_TAG,
                null);
        //
        URI uri = instance.addRevision(CLOUD_ID, REPRESENTATION_NAME, VERSION, REVISION_NAME, PROVIDER_ID, Tags.ACCEPTANCE.getTag());
        assertNotNull(uri);
        assertEquals(EXPECTED_REVISION_PATH_WITH_ACCEPTED_TAG, uri.toString());
    }

    @Test
    public void shouldAddRevisionWithPublishedTag() throws MCSException {
        //
        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/test_cloud_id/representations/test_representation/versions/de084210-a393-11e3-8614-50e549e85271/revisions/test_revision_name/revisionProvider/test_provider_id/tag/published",
                201,
                EXPECTED_REVISION_PATH_WITH_PUBLISHED_TAG,
                null
        );
        //
        URI uri = instance.addRevision(CLOUD_ID, REPRESENTATION_NAME, VERSION, REVISION_NAME, PROVIDER_ID, Tags.PUBLISHED.getTag());
        assertNotNull(uri);
        assertEquals(EXPECTED_REVISION_PATH_WITH_PUBLISHED_TAG, uri.toString());
    }

    @Test
    public void shouldAddRevisionWithDeletedTag() throws MCSException {
        //
        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/test_cloud_id/representations/test_representation/versions/de084210-a393-11e3-8614-50e549e85271/revisions/test_revision_name/revisionProvider/test_provider_id/tag/deleted",
                201,
                EXPECTED_REVISION_PATH_WITH_DELETED_TAG,
                null
        );
        //
        URI uri = instance.addRevision(CLOUD_ID, REPRESENTATION_NAME, VERSION, REVISION_NAME, PROVIDER_ID, Tags.DELETED.getTag());
        assertNotNull(uri);
        assertEquals(EXPECTED_REVISION_PATH_WITH_DELETED_TAG, uri.toString());
    }

    @Test
    public void shouldAddRevisionWithMultipleTags() throws MCSException {
        //
        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/test_cloud_id/representations/test_representation/versions/de084210-a393-11e3-8614-50e549e85271/revisions/test_revision_name/revisionProvider/test_provider_id/tags",
                201,
                EXPECTED_REVISION_PATH_MULTIPLE_TAGS,
                null);
        //
        Set<Tags> tags = new HashSet<>();
        tags.add(Tags.ACCEPTANCE);
        tags.add(Tags.DELETED);
        URI uri = instance.addRevision(CLOUD_ID, REPRESENTATION_NAME, VERSION, REVISION_NAME, PROVIDER_ID, tags);
        assertNotNull(uri);
        assertEquals(EXPECTED_REVISION_PATH_MULTIPLE_TAGS, uri.toString());

    }

    @Test
    public void shouldRemoveRevision() throws MCSException {
        //
        new WiremockHelper(wireMockRule).stubDelete(
                "/mcs/records/test_cloud_id/representations/test_representation/versions/de084210-a393-11e3-8614-50e549e85271/revisions/test_revision_name/revisionProvider/test_provider_id?revisionTimestamp=2019-07-11",
                204);
        //
        instance.deleteRevision(CLOUD_ID, REPRESENTATION_NAME, VERSION, REVISION_NAME, PROVIDER_ID, "2019-07-11");
        assertTrue(true);
    }


    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowPresentationDoesNotExistsException() throws MCSException {
        //
        new WiremockHelper(wireMockRule).stubDelete(
                "/mcs/records/test_cloud_id/representations/REP_NOT_FOUND/versions/de084210-a393-11e3-8614-50e549e85271/revisions/test_revision_name/revisionProvider/test_provider_id?revisionTimestamp=2019-07-11",
                404,
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>REPRESENTATION_NOT_EXISTS</errorCode></errorInfo>");
        //
        instance.deleteRevision(CLOUD_ID, "REP_NOT_FOUND", VERSION, REVISION_NAME, PROVIDER_ID, "2019-07-11");
    }


    @Test
    public void shouldAddRevisionWithEmptyTagsSet() throws MCSException {
        //
        new WiremockHelper(wireMockRule).stubPost(
                "/mcs/records/test_cloud_id/representations/test_representation/versions/de084210-a393-11e3-8614-50e549e85271/revisions/test_revision_name/revisionProvider/test_provider_id/tags",
                201,
                EXPECTED_REVISION_PATH_MULTIPLE_TAGS,
                null);
        //
        Set<Tags> tags = new HashSet<>();
        URI uri = instance.addRevision(CLOUD_ID, REPRESENTATION_NAME, VERSION, REVISION_NAME, PROVIDER_ID, tags);
        assertNotNull(uri);
        assertEquals(EXPECTED_REVISION_PATH_MULTIPLE_TAGS, uri.toString());
    }
}