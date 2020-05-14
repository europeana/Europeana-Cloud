package eu.europeana.cloud.mcs.driver;


import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.http.MediaType;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;


@Ignore
public class FileServiceClientITest {

    private  static final String LOCAL_TEST_URL = "http://localhost:8080/mcs";
    private  static final String LOCAL_TEST_UIS_URL = "http://localhost:8080/uis";
    private  static final String REMOTE_TEST_URL = "https://test.ecloud.psnc.pl/api";
    private  static final String REMOTE_TEST_UIS_URL = "https://test.ecloud.psnc.pl/api";

    private static final String USER_NAME = "metis_test";  //user z bazy danych
    private static final String USER_PASSWORD = "1RkZBuVf";
    private static final String ADMIN_NAME = "admin";  //admin z bazy danych
    private static final String ADMIN_PASSWORD = "glEumLWDSVUjQcRVswhN";

    @Test
    public void getFile1() throws MCSException, IOException{
        String fileUrlText = "http://localhost:8080/mcs/<enter_path_to_file_here>";

        FileServiceClient mcsClient = new FileServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
        InputStream resultInputStream = mcsClient.getFile(fileUrlText);

        assertNotNull(resultInputStream);
    }

    @Test
    public void getFile3() throws MCSException, IOException{
        String fileUrlText = "http://localhost:8080/mcs/<enter_path_to_file_here>";

        FileServiceClient mcsClient = new FileServiceClient(LOCAL_TEST_URL);
        InputStream resultInputStream = mcsClient.getFile(fileUrlText,
                MCSClient.AUTHORIZATION_KEY, MCSClient.getAuthorisationValue(USER_NAME, USER_PASSWORD));

        assertNotNull(resultInputStream);
    }

    @Test
    public void uploadFile() throws MCSException, IOException {
        String cloudId = "<enter_cloud_id_here>";
        String representationName = "<enter_representation_name_here>";
        String version = "<enter_version_here>";

        String filename = "log4j.properties";
        InputStream is = FileServiceClientITest.class.getResourceAsStream("/"+filename);
        String mimeType = MediaType.TEXT_PLAIN_VALUE;

        FileServiceClient mcsClient = new FileServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
        URI resultUri = mcsClient.uploadFile(cloudId, representationName, version, filename, is, mimeType);

        assertNotNull(resultUri);
    }
}
