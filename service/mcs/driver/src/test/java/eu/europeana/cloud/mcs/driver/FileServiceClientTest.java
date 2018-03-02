package eu.europeana.cloud.mcs.driver;

import co.freeside.betamax.Betamax;
import co.freeside.betamax.Recorder;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.service.mcs.exception.*;
import org.junit.Rule;
import org.junit.Test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URI;

import static java.util.Arrays.copyOfRange;
import static org.junit.Assert.*;

public class FileServiceClientTest {

    @Rule
    public Recorder recorder = new Recorder();

    //TODO clean
    //this is only needed for recording tests
    private static final String BASE_URL_ISTI = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/";
    private static final String BASE_URL_LOCALHOST = "http://localhost:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT";
    
    private static final String baseUrl = BASE_URL_ISTI;
    
    private static final String mediaType = "text/plain";
    private static final String cloudId = "7MZWQJF8P84";
    private static final String representationName = "schema_000001";
    private static final String version = "de084210-a393-11e3-8614-50e549e85271";

    private static final String unmovableFileName = "9007c26f-e29d-4924-9c49-8ff064484264";
    private static final String deletedFileName = "d64b423b-1018-4526-ab4b-3539261ff067";
    private static final String modyfiedFileName = "12c068c9-461d-484e-878f-099c5fca447f";
    
    private static final String UPLOADED_FILE_CONTENTS = "Test_123456789_";
    private static final String MODIFIED_FILE_CONTENTS = "Test_123456789_123456";
    
    /** Should already exist in the system */
    private static final String CLOUD_ID = "W3KBLNZDKNQ";
    private static final String PROVIDER_ID = "Provider001";
    
    private static final String REPRESENTATION_NAME = "schema66";
    private static final String UPLOADED_FILE_NAME = "9007c26f-e29d-4924-9c49-8ff064484264";
    private static final String MODIFIED_FILE_NAME = "06abeac8-6221-4399-be68-5be5ae8d1473";
    private static final String DELETED_FILE_NAME = "b32b56e9-94d7-44b8-9010-7a1795ee7f95";
    
    //this is some not-persistent version
    private static final String VERSION = "e91d6300-431c-11e4-8576-00163eefc9c8";

	private static final String PERSISTED_VERSION = "881c5c00-4259-11e4-9c35-00163eefc9c8";

    private static Client client;
    //records/CLOUDID/representations/REPRESENTATIONNAME/versions/VERSION/files/
    private static final String filesPath = "records/{" + ParamConstants.P_CLOUDID + "}/representations/{"
            + ParamConstants.P_REPRESENTATIONNAME + "}/versions/{" + ParamConstants.P_VER + "}/files";
    //records/CLOUDID/representations/REPRESENTATIONNAME/versions/VERSION/files/FILENAME/
    private static final String filePath = filesPath + "/{" + ParamConstants.P_FILENAME + "}";
    
    private static final String username = "Cristiano";
    private static final String password = "Ronaldo";


    @Betamax(tape = "files/shouldGetFileWithoutRange")
    @Test
    public void shouldGetFileWithoutRange()
            throws UnsupportedEncodingException, MCSException, IOException {
        byte[] contentBytes = MODIFIED_FILE_CONTENTS.getBytes("UTF-8");
        String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);
        
        InputStream responseStream = instance.getFile(CLOUD_ID, REPRESENTATION_NAME, VERSION, UPLOADED_FILE_NAME);

        assertNotNull(responseStream);
        byte[] responseBytes = ByteStreams.toByteArray(responseStream);
        assertArrayEquals("Content is incorrect", contentBytes, responseBytes);
        String responseChecksum = Hashing.md5().hashBytes(responseBytes).toString();
        assertEquals("Checksum is incorrect", contentChecksum, responseChecksum);
    }


    @Betamax(tape = "files/shouldGetFileWithRange1")
    @Test
    public void shouldGetFileWithRange1()
            throws UnsupportedEncodingException, MCSException, IOException {
        getFileWithRange(1, 2);
    }


    @Betamax(tape = "files/shouldGetFileWithRange2")
    @Test
    public void shouldGetFileWithRange2()
            throws UnsupportedEncodingException, MCSException, IOException {
        getFileWithRange(3, 14);
    }


    @Betamax(tape = "files/shouldGetFileWithRange3")
    @Test
    public void shouldGetFileWithRange3()
            throws UnsupportedEncodingException, MCSException, IOException {
        getFileWithRange(0, 10);
    }


    @Betamax(tape = "files/shouldGetFileWithRange4")
    @Test
    public void shouldGetFileWithRange4()
            throws UnsupportedEncodingException, MCSException, IOException {
        getFileWithRange(0, 0);
    }


    //Function required because Betamax does not compatible with multiple requests
    private static void getFileWithRange(Integer rangeStart, Integer rangeEnd)
            throws UnsupportedEncodingException, MCSException, IOException {
    	
    	String fileName = "f5b0cd7f-f8ec-4834-8537-b7ff3171279b";
    	
        String contentString = "Test_123456789_8";
        byte[] contentBytes = contentString.getBytes("UTF-8");
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);
        String range = String.format("bytes=%d-%d", rangeStart, rangeEnd);
        
//        InputStream responseStream = instance.getFile(cloudId, representationName, version, unmovableFileName, range);
        InputStream responseStream = instance.getFile(CLOUD_ID, REPRESENTATION_NAME, VERSION, UPLOADED_FILE_NAME, range);

        assertNotNull(responseStream);
        byte[] responseBytes = ByteStreams.toByteArray(responseStream);
        byte[] rangedContentBytes = copyOfRange(contentBytes, rangeStart, rangeEnd + 1);
        assertArrayEquals("Content is incorrect", rangedContentBytes, responseBytes);
    }


    @Betamax(tape = "files/shouldThrowWrongContentRangeExceptionForGetFileWithRangeWhenIncorrectFormat")
    @Test(expected = WrongContentRangeException.class)
    public void shouldThrowWrongContentRangeExceptionForGetFileWithRangeWhenIncorrectFormat()
            throws UnsupportedEncodingException, MCSException, IOException {
        int rangeStart = 1;
        int rangeEnd = 4;
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);
        String range = String.format("bytese=%d-%d", rangeStart, rangeEnd);

        instance.getFile(CLOUD_ID, REPRESENTATION_NAME, VERSION, unmovableFileName, range);
    }


    @Betamax(tape = "files/shouldThrowWrongContentRangeExceptionForGetFileWithRangeWhenIncorrectRangeValues")
    @Test(expected = WrongContentRangeException.class)
    public void shouldThrowWrongContentRangeExceptionForGetFileWithRangeWhenIncorrectRangeValues()
            throws UnsupportedEncodingException, MCSException, IOException {
        int rangeStart = 1;
        int rangeEnd = 50;
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);
        String range = String.format("bytese=%d-%d", rangeStart, rangeEnd);

        instance.getFile(CLOUD_ID, REPRESENTATION_NAME, VERSION, unmovableFileName, range);
    }


    @Betamax(tape = "files/shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForGetFileWithoutRange")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForGetFileWithoutRange()
            throws UnsupportedEncodingException, MCSException, IOException {
        String incorrectFileName = "edefc11e-1c5f-4a71-adb6-28efdd7b3b00";
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.getFile(cloudId, representationName, version, incorrectFileName);
    }


    @Betamax(tape = "files/shouldThrowRepresentationNotExistsForGetFileWithoutRangeWhenIncorrectCloudId")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowRepresentationNotExistsForGetFileWithoutRangeWhenIncorrectCloudId()
            throws UnsupportedEncodingException, MCSException, IOException {
        String incorrectCloudId = "7MZWQJF8P99";
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.getFile(incorrectCloudId, representationName, version, unmovableFileName);
    }


    @Betamax(tape = "files/shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForGetFileWithoutRangeWhenIncorrectRepresentationName")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForGetFileWithoutRangeWhenIncorrectRepresentationName()
            throws UnsupportedEncodingException, MCSException, IOException {
        String incorrectRepresentationName = "schema_000101";
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.getFile(cloudId, incorrectRepresentationName, version, unmovableFileName);
    }


    @Betamax(tape = "files/shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForGetFileWithoutRangeWhenIncorrectVersion")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForGetFileWithoutRangeWhenIncorrectVersion()
            throws UnsupportedEncodingException, MCSException, IOException {
        String incorrectVersion = "8a64f9b0-98b6-11e3-b072-50e549e85200";
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.getFile(cloudId, representationName, incorrectVersion, unmovableFileName);
    }


    @Betamax(tape = "files/shouldUploadFile")
    @Test
    public void shouldUploadFile()
            throws UnsupportedEncodingException, MCSException, IOException {
        String contentString = UPLOADED_FILE_CONTENTS;
        byte[] contentBytes = contentString.getBytes("UTF-8");
        InputStream contentStream = new ByteArrayInputStream(contentBytes);
        String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        URI uri = instance.uploadFile(CLOUD_ID, REPRESENTATION_NAME, VERSION, contentStream, mediaType);
        System.out.println(uri);
        assertNotNull(uri);
        
        // TODO: this stuff cannot be accessed any more, as user must be authenticated ->
        
//        Response response = client.target(uri).request().get();
//        InputStream responseStream = response.readEntity(InputStream.class);
//        String responseChecksum = response.getEntityTag().toString();
//        assertNotNull(responseStream);
//        assertEquals("Checksum is incorrect", contentChecksum,
//            responseChecksum.subSequence(1, responseChecksum.length() - 1).toString());
//        assertArrayEquals("Content is incorrect", contentBytes, ByteStreams.toByteArray(responseStream));
    }


    @Betamax(tape = "files/shouldThrowRepresentationNotExistsExceptionForUploadFileWhenIncorrectCloudId")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsExceptionForUploadFileWhenIncorrectCloudId()
            throws MCSException, IOException {
        String contentString = "Test_123456789_";
        String incorrectCloudId = "7MZWQJS8P84";
        byte[] contentBytes = contentString.getBytes("UTF-8");
        InputStream contentStream = new ByteArrayInputStream(contentBytes);
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.uploadFile(incorrectCloudId, representationName, version, contentStream, mediaType);
    }


    @Betamax(tape = "files/shouldThrowRepresentationNotExistsExceptionForUploadFileWhenIncorrectRepresentationName")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsExceptionForUploadFileWhenIncorrectRepresentationName()
            throws MCSException, IOException {
        String contentString = "Test_123456789_";
        String incorrectRepresentationName = "schema_000101";
        byte[] contentBytes = contentString.getBytes("UTF-8");
        InputStream contentStream = new ByteArrayInputStream(contentBytes);
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.uploadFile(cloudId, incorrectRepresentationName, version, contentStream, mediaType);
    }


    @Betamax(tape = "files/shouldThrowRepresentationNotExistsExceptionForUploadFileWhenIncorrectVersion")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsExceptionForUploadFileWhenIncorrectVersion()
            throws MCSException, IOException {
        String contentString = "Test_123456789_";
        String incorrectVersion = "8a64f9b0-98b6-11e3-b072-50e549e85200";
        byte[] contentBytes = contentString.getBytes("UTF-8");
        InputStream contentStream = new ByteArrayInputStream(contentBytes);
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.uploadFile(cloudId, representationName, incorrectVersion, contentStream, mediaType);
    }



    @Betamax(tape = "files/shouldThrowCannotModifyPersistentRepresentationExceptionForUploadFile")
    @Test(expected = CannotModifyPersistentRepresentationException.class)
    public void shouldThrowCannotModifyPersistentRepresentationExceptionForUploadFile()
            throws MCSException, IOException {
        String contentString = "Test_123456789_";
        String persistedVersion = "80441ab0-a38d-11e3-8614-50e549e85271";
        byte[] contentBytes = contentString.getBytes("UTF-8");
        InputStream contentStream = new ByteArrayInputStream(contentBytes);
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.uploadFile(CLOUD_ID, REPRESENTATION_NAME, PERSISTED_VERSION, contentStream, mediaType);
    }


    @Betamax(tape = "files/shouldUploadFileWithChecksum")
    @Test
    public void shouldUploadFileWithChecksum()
            throws UnsupportedEncodingException, MCSException, IOException {
        String contentString = "Test_123456789_1";
        byte[] contentBytes = contentString.getBytes("UTF-8");
        InputStream contentStream = new ByteArrayInputStream(contentBytes);
        String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        URI uri = instance.uploadFile(CLOUD_ID, REPRESENTATION_NAME, VERSION, contentStream, mediaType, contentChecksum);
        assertNotNull(uri);
        
        // TODO: this stuff cannot be accessed any more, as user must be authenticated ->
        
//        Response response = client.target(uri).request().get();
//        InputStream responseStream = response.readEntity(InputStream.class);
//        String responseChecksum = response.getEntityTag().toString();
//        assertNotNull(responseStream);
//        assertEquals("Checksum is incorrect", contentChecksum,
//            responseChecksum.subSequence(1, responseChecksum.length() - 1).toString());
//        assertArrayEquals("Content is incorrect", contentBytes, ByteStreams.toByteArray(responseStream));
    }




    @Betamax(tape = "files/shouldThrowRepresentationNotExistsExceptionForUploadFileWithChecksumWhenIncorrectCloudId")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsExceptionForUploadFileWithChecksumWhenIncorrectCloudId()
            throws UnsupportedEncodingException, MCSException, IOException {
        String incorrectCloudId = "7MZWQJF8P00";
        String contentString = "Test_123456789_1";
        byte[] contentBytes = contentString.getBytes("UTF-8");
        InputStream contentStream = new ByteArrayInputStream(contentBytes);
        String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.uploadFile(incorrectCloudId, representationName, version, contentStream, mediaType, contentChecksum);
    }


    @Betamax(tape = "files/shouldThrowRepresentationNotExistsExceptionForUploadFileWithChecksumWhenIncorrectRepresentationName")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsExceptionForUploadFileWithChecksumWhenIncorrectRepresentationName()
            throws UnsupportedEncodingException, MCSException, IOException {
        String incorrectRepresentationName = "schema_000101";
        String contentString = "Test_123456789_1";
        byte[] contentBytes = contentString.getBytes("UTF-8");
        InputStream contentStream = new ByteArrayInputStream(contentBytes);
        String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.uploadFile(cloudId, incorrectRepresentationName, version, contentStream, mediaType, contentChecksum);
    }


    @Betamax(tape = "files/shouldThrowRepresentationNotExistsExceptionForUploadFileWithChecksumWhenIncorrectVersion")
    @Test(expected = RepresentationNotExistsException.class)
    public void shouldThrowRepresentationNotExistsExceptionForUploadFileWithChecksumWhenIncorrectVersion()
            throws UnsupportedEncodingException, MCSException, IOException {
        String incorrectVersion = "8a64f9b0-98b6-11e3-b072-50e549e85200";
        String contentString = "Test_123456789_1";
        byte[] contentBytes = contentString.getBytes("UTF-8");
        InputStream contentStream = new ByteArrayInputStream(contentBytes);
        String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.uploadFile(cloudId, representationName, incorrectVersion, contentStream, mediaType, contentChecksum);
    }


    @Betamax(tape = "files/shouldThrowCannotModifyPersistentRepresentationExceptionForUploadFileWithChecksum")
    @Test(expected = CannotModifyPersistentRepresentationException.class)
    public void shouldThrowCannotModifyPersistentRepresentationExceptionForUploadFileWithChecksum()
            throws UnsupportedEncodingException, MCSException, IOException {
        String persistedVersion = "80441ab0-a38d-11e3-8614-50e549e85271";
        String contentString = "Test_123456789_1";
        byte[] contentBytes = contentString.getBytes("UTF-8");
        InputStream contentStream = new ByteArrayInputStream(contentBytes);
        String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.uploadFile(CLOUD_ID, REPRESENTATION_NAME, PERSISTED_VERSION, contentStream, mediaType, contentChecksum);
    }


    @Betamax(tape = "files/shouldThrowIOExceptionForUploadFile")
    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionForUploadFile()
            throws UnsupportedEncodingException, MCSException, IOException {
        String contentString = "Test_123456789_1";
        byte[] contentBytes = contentString.getBytes("UTF-8");
        InputStream contentStream = new ByteArrayInputStream(contentBytes);
        String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
        String incorrectContentChecksum = contentChecksum.substring(1) + "0";
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.uploadFile(CLOUD_ID, REPRESENTATION_NAME, VERSION, contentStream, mediaType, incorrectContentChecksum);
    }


    @Betamax(tape = "files/shouldModifyFile")
    @Test
    public void shouldModifyFile()
            throws UnsupportedEncodingException, IOException, MCSException {
        String contentString = MODIFIED_FILE_CONTENTS;
        byte[] contentBytes = contentString.getBytes("UTF-8");
        InputStream contentStream = new ByteArrayInputStream(contentBytes);
        String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        URI uri = instance.modyfiyFile(CLOUD_ID, REPRESENTATION_NAME, VERSION, contentStream, mediaType,
        		MODIFIED_FILE_NAME, contentChecksum);

        assertNotNull(uri);
        
        // TODO: this stuff cannot be accessed any more, as user must be authenticated ->
        
//        Response response = client.target(uri).request().get();
//        InputStream responseStream = response.readEntity(InputStream.class);
//        String responseChecksum = response.getEntityTag().toString();
//        assertNotNull(responseStream);
//        assertEquals("Checksum is incorrect", contentChecksum,
//            responseChecksum.subSequence(1, responseChecksum.length() - 1).toString());
//        assertArrayEquals("Content is incorrect", contentBytes, ByteStreams.toByteArray(responseStream));
    }


    @Betamax(tape = "files/shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForModifyFileWhenIncorrectCloudId")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForModifyFileWhenIncorrectCloudId()
            throws UnsupportedEncodingException, IOException, MCSException {
        String incorrectCloudId = "12c068c9-461d-484e-878f-099c5fca4400";
        String contentString = "Test_123456789_123456";
        byte[] contentBytes = contentString.getBytes("UTF-8");
        InputStream contentStream = new ByteArrayInputStream(contentBytes);
        String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.modyfiyFile(incorrectCloudId, representationName, version, contentStream, mediaType, modyfiedFileName,
            contentChecksum);
    }


    @Betamax(tape = "files/shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForModifyFileWhenIncorrectRepresentationName")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForModifyFileWhenIncorrectRepresentationName()
            throws UnsupportedEncodingException, IOException, MCSException {
        String incorrectRepresentationName = "schema_000101";
        String contentString = "Test_123456789_123456";
        byte[] contentBytes = contentString.getBytes("UTF-8");
        InputStream contentStream = new ByteArrayInputStream(contentBytes);
        String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.modyfiyFile(cloudId, incorrectRepresentationName, version, contentStream, mediaType, modyfiedFileName,
            contentChecksum);
    }


    @Betamax(tape = "files/shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForModifyFileWhenIncorrectVersion")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForModifyFileWhenIncorrectVersion()
            throws UnsupportedEncodingException, IOException, MCSException {
        String incorrectVersion = "8a64f9b0-98b6-11e3-b072-50e549e85200";
        String contentString = "Test_123456789_123456";
        byte[] contentBytes = contentString.getBytes("UTF-8");
        InputStream contentStream = new ByteArrayInputStream(contentBytes);
        String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.modyfiyFile(cloudId, representationName, incorrectVersion, contentStream, mediaType, modyfiedFileName,
            contentChecksum);
    }

    @Betamax(tape = "files/shouldThrowIOExceptionForModifyFile")
    @Test(expected = IOException.class)
    public void shouldThrowIOExceptionForModifyFile()
            throws UnsupportedEncodingException, IOException, MCSException {
        String contentString = "Test_123456789_123456";
        byte[] contentBytes = contentString.getBytes("UTF-8");
        InputStream contentStream = new ByteArrayInputStream(contentBytes);
        String contentChecksum = Hashing.md5().hashBytes(contentBytes).toString();
        String incorrectContentChecksum = contentChecksum.substring(1) + "0";
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.modyfiyFile(CLOUD_ID, REPRESENTATION_NAME, VERSION, contentStream, mediaType, MODIFIED_FILE_NAME,
            incorrectContentChecksum);
    }



    @Betamax(tape = "files/shouldDeleteFile")
    //@Test
    public void shouldDeleteFile()
            throws MCSException {
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.deleteFile(CLOUD_ID, REPRESENTATION_NAME, VERSION, UPLOADED_FILE_CONTENTS);

        Response response = BuildWebTarget(cloudId, representationName, version, deletedFileName).request().get();
        assertEquals("", Response.Status.NOT_FOUND.getStatusCode(), response.getStatus());
    }


    @Betamax(tape = "files/shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForDeleteFileWhenIncorrectCloudId")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForDeleteFileWhenIncorrectCloudId()
            throws MCSException {
        String incorrectCloudId = "7MZWQJF8P99";
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.deleteFile(incorrectCloudId, representationName, version, deletedFileName);
    }


    @Betamax(tape = "files/shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForDeleteFileWhenIncorrectRepresentationName")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForDeleteFileWhenIncorrectRepresentationName()
            throws MCSException {
        String incorrectRepresentationName = "schema_000101";
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.deleteFile(cloudId, incorrectRepresentationName, version, deletedFileName);
    }


    @Betamax(tape = "files/shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForDeleteFileWhenIncorrectVersion")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForDeleteFileWhenIncorrectVersion()
            throws MCSException {
        String incorrectVersion = "8a64f9b0-98b6-11e3-b072-50e549e85200";
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.deleteFile(cloudId, representationName, incorrectVersion, deletedFileName);
    }


    @Betamax(tape = "files/shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForDeleteFile")
    @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
    public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForDeleteFile()
            throws MCSException {
        String notExistDeletedFileName = "d64b423b-1018-4526-ab4b-3539261ff000";
        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.deleteFile(cloudId, representationName, version, notExistDeletedFileName);
    }


    @Betamax(tape = "files/shouldThrowCannotModifyPersistentRepresentationExceptionForDeleteFile")
    @Test(expected = CannotModifyPersistentRepresentationException.class)
    public void shouldThrowCannotModifyPersistentRepresentationExceptionForDeleteFile()
            throws MCSException, UnsupportedEncodingException {

        FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

        instance.deleteFile(CLOUD_ID, REPRESENTATION_NAME, "eb5c0a60-4306-11e4-8576-00163eefc9c8", DELETED_FILE_NAME);
    }


    private static WebTarget BuildWebTarget(String cloudId, String schema, String version, String fileName) {
        return client.target(baseUrl).path(filePath).resolveTemplate(ParamConstants.P_CLOUDID, cloudId)
                .resolveTemplate(ParamConstants.P_REPRESENTATIONNAME, schema)
                .resolveTemplate(ParamConstants.P_VER, version).resolveTemplate(ParamConstants.P_FILENAME, fileName);
    }
}
