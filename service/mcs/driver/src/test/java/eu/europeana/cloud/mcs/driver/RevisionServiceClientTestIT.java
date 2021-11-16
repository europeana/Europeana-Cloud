package eu.europeana.cloud.mcs.driver;

import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.junit.Test;

import java.net.URI;
import java.util.Date;
import java.util.GregorianCalendar;

import static org.junit.Assert.*;

public class RevisionServiceClientTestIT {
    private  static final String LOCAL_TEST_URL = "http://localhost:8080/mcs";
    private  static final String LOCAL_TEST_UIS_URL = "http://localhost:8080/uis";
    private  static final String REMOTE_TEST_URL = "https://test.ecloud.psnc.pl/api";
    private  static final String REMOTE_TEST_UIS_URL = "https://test.ecloud.psnc.pl/api";

    private static final String USER_NAME = "metis_test";  //user z bazy danych
    private static final String USER_PASSWORD = "1RkZBuVf";
    private static final String ADMIN_NAME = "admin";  //admin z bazy danych
    private static final String ADMIN_PASSWORD = "glEumLWDSVUjQcRVswhN";

    @Test
    public void addRevision() throws MCSException {
        String cloudId = "enter_cloud_id_here";
        String representationName = "enter_representation_name_here";
        String version = "enter_version_here";

        String revisionName = "<enter_revision_name_here>";
        String revisionProviderId = "<enter_revision_provider_id_here>";
        Date creationTimeStamp = GregorianCalendar.getInstance().getTime();
        boolean acceptance = true;
        boolean published = true;
        boolean deleted = false;
        Revision revision = new Revision(
                revisionName,
                revisionProviderId,
                creationTimeStamp,
                acceptance,
                published,
                deleted
        );

        RevisionServiceClient mcsClient = new RevisionServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
        URI revisionUri = mcsClient.addRevision(cloudId, representationName, version, revision,
                MCSClient.AUTHORIZATION_KEY, MCSClient.getAuthorisationValue(USER_NAME, USER_PASSWORD));

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
        mcsClient.deleteRevision(cloudId, representationName, version, revisionName, revisionProviderId, timestamp);

        assertTrue(true);
    }

}
