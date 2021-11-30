package eu.europeana.cloud.mcs.driver;


import com.google.common.hash.Hashing;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.commons.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.http.MediaType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;


@Ignore
public class FileServiceClientITest {

    private static final String LOCAL_TEST_URL = "http://localhost:8080/mcs";
    private static final String LOCAL_TEST_UIS_URL = "http://localhost:8080/uis";
    private static final String REMOTE_TEST_URL = "https://test.ecloud.psnc.pl/api";
    private static final String REMOTE_TEST_UIS_URL = "https://test.ecloud.psnc.pl/api";

    private static final String USER_NAME = "metis_test";  //user z bazy danych
    private static final String USER_PASSWORD = "1RkZBuVf";
    private static final String ADMIN_NAME = "admin";  //admin z bazy danych
    private static final String ADMIN_PASSWORD = "glEumLWDSVUjQcRVswhN";

    @Test
    public void getFile1() throws MCSException, IOException {
        String fileUrlText = "http://localhost:8080/mcs/<enter_path_to_file_here>";

        FileServiceClient mcsClient = new FileServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
        InputStream resultInputStream = mcsClient.getFile(fileUrlText);

        assertNotNull(resultInputStream);
    }

    @Test
    public void getFile3() throws MCSException, IOException {
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
        InputStream is = FileServiceClientITest.class.getResourceAsStream("/" + filename);
        String mimeType = MediaType.TEXT_PLAIN_VALUE;

        FileServiceClient mcsClient = new FileServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);
        URI resultUri = mcsClient.uploadFile(cloudId, representationName, version, filename, is, mimeType);

        assertNotNull(resultUri);
    }


    @Test
    public void uploadFile_verifyValidBinaryDataStored() throws Exception {

        String cloudId = "7XGEDN7JTPRL6SALCRQDG4WX5CYRZTFJ6GDXJKLAAZHHJNSUCMSA";
        String representationName = "tekstowy";
        String version = "41caee00-3db3-11ea-8db6-04922659f621";
        String mediaType = MediaType.APPLICATION_OCTET_STREAM_VALUE;
        String fileName = "file_service_client_it_random_binary_file.txt";

        byte[] content = new byte[1000000];
        ThreadLocalRandom.current().nextBytes(content);
        String contentMd5 = Hashing.md5().hashBytes(content).toString();

        FileServiceClient mcsClient = new FileServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

        try {
            mcsClient.deleteFile(cloudId, representationName, version, fileName);
        } catch (Exception e) {
            //Ignore not found
        }


        URI resultUri = null;

        ByteArrayInputStream inputStream = new ByteArrayInputStream(content);
        //Uncoment one of them to test and comment modification section
        resultUri = mcsClient.uploadFile(cloudId, representationName, version, fileName, inputStream, mediaType);
        //     resultUri = mcsClient.uploadFile(cloudId, representationName, version, inputStream, mediaType, contentMd5);
        //    resultUri = mcsClient.uploadFile(cloudId, representationName, version, inputStream, mediaType);
        //     resultUri = mcsClient.uploadFile("http://localhost:8080/mcs/records/7XGEDN7JTPRL6SALCRQDG4WX5CYRZTFJ6GDXJKLAAZHHJNSUCMSA/representations/tekstowy/versions/41caee00-3db3-11ea-8db6-04922659f621",inputStream,mediaType);


        //Modifications
        content[120631] = 77;
        content[140631] = 81;
        contentMd5 = Hashing.md5().hashBytes(content).toString();
        inputStream = new ByteArrayInputStream(content);

        //Uncoment one of them to test
        //  mcsClient.modyfiyFile(cloudId, representationName, version, inputStream,  mediaType, fileName, contentMd5);
        resultUri = mcsClient.modifyFile(resultUri.toString(), inputStream, mediaType);
        System.out.println("fileUri5: " + resultUri);

        InputStream resultStream = mcsClient.getFile(resultUri.toString());

        byte[] result = IOUtils.toByteArray(resultStream);
        assertArrayEquals(content, result);
    }

    @Test
    public void getFile_throwsFileNotExistsExceptionWhileFileNotExists() {

        String cloudId = "7XGEDN7JTPRL6SALCRQDG4WX5CYRZTFJ6GDXJKLAAZHHJNSUCMSA";
        String representationName = "tekstowy";
        String version = "41caee00-3db3-11ea-8db6-04922659f621";
       // String fileName = "notFoundedABC.xml"; //Good file name
        String fileName = "notFoundedABC.txt";

        FileServiceClient mcsClient = new FileServiceClient(LOCAL_TEST_URL, USER_NAME, USER_PASSWORD);

        try {
            mcsClient.getFile(cloudId, representationName, version, fileName);

        } catch (FileNotExistsException e) {
            System.out.println("Valid exception:" + e);
        } catch (Exception e) {
            e.printStackTrace();
            fail("Unexpected exception");
            //Ignore not found
        }

    }
}