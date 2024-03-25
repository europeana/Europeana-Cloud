package eu.europeana.cloud.mcs.driver;

import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static java.util.Arrays.copyOfRange;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.google.common.io.ByteStreams;
import eu.europeana.cloud.service.mcs.exception.AccessDeniedOrObjectDoesNotExistException;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.WrongContentRangeException;
import eu.europeana.cloud.test.WiremockHelper;
import jakarta.xml.bind.DatatypeConverter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.junit.Rule;
import org.junit.Test;

public class FileServiceClientTest {

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(wireMockConfig().port(8080));

  private static final String baseUrl = "http://127.0.0.1:8080/mcs";

  private static final String mediaType = "text/plain";
  private static final String cloudId = "7MZWQJF8P84";
  private static final String representationName = "schema_000001";
  private static final String version = "de084210-a393-11e3-8614-50e549e85271";

  private static final String unmovableFileName = "9007c26f-e29d-4924-9c49-8ff064484264";
  private static final String deletedFileName = "d64b423b-1018-4526-ab4b-3539261ff067";
  private static final String modyfiedFileName = "12c068c9-461d-484e-878f-099c5fca447f";

  private static final String UPLOADED_FILE_CONTENTS = "Test_123456789_";
  private static final String MODIFIED_FILE_CONTENTS = "Test_123456789_123456";

  /**
   * Should already exist in the system
   */
  private static final String TEST_CLOUD_ID = "W3KBLNZDKNQ";

  private static final String TEST_REPRESENTATION_NAME = "schema66";
  private static final String UPLOADED_FILE_NAME = "9007c26f-e29d-4924-9c49-8ff064484264";
  private static final String MODIFIED_FILE_NAME = "06abeac8-6221-4399-be68-5be5ae8d1473";
  private static final String DELETED_FILE_NAME = "b32b56e9-94d7-44b8-9010-7a1795ee7f95";

  //this is some not-persistent version
  private static final String TEST_VERSION = "e91d6300-431c-11e4-8576-00163eefc9c8";

  private static final String PERSISTED_VERSION = "881c5c00-4259-11e4-9c35-00163eefc9c8";

  private static final String username = "Cristiano";
  private static final String password = "Ronaldo";

  @Test
  public void shouldGetFileWithoutRange() throws MCSException, IOException, NoSuchAlgorithmException {
    byte[] contentBytes = MODIFIED_FILE_CONTENTS.getBytes(StandardCharsets.UTF_8);
    String contentChecksum = createMD5(contentBytes);

    //
    new WiremockHelper(wireMockRule).stubGet(
        "/mcs/records/7MZWQJF8P84/representations/schema_000001/versions/de084210-a393-11e3-8614-50e549e85271/files/08fcc281-e1fd-4cec-bd33-c12a49145d36",
        200,
        "Test_123456789_8");
    new WiremockHelper(wireMockRule).stubGet(
        "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files/9007c26f-e29d-4924-9c49-8ff064484264",
        200,
        "Test_123456789_123456");
    //

    FileServiceClient instance = new FileServiceClient("http://127.0.0.1:8080/mcs", username, password);

    InputStream responseStream = instance.getFile(TEST_CLOUD_ID, TEST_REPRESENTATION_NAME, TEST_VERSION, UPLOADED_FILE_NAME);

    assertNotNull(responseStream);
    byte[] responseBytes = ByteStreams.toByteArray(responseStream);
    assertArrayEquals("Content is incorrect", contentBytes, responseBytes);
    String responseChecksum = createMD5(responseBytes);
    assertEquals("Checksum is incorrect", contentChecksum, responseChecksum);
  }


  @Test
  public void shouldGetFileWithRange1() throws MCSException, IOException {
    //
    new WiremockHelper(wireMockRule).stubGet(
        "mcs/records/7MZWQJF8P84/representations/schema_000001/versions/de084210-a393-11e3-8614-50e549e85271/files/08fcc281-e1fd-4cec-bd33-c12a49145d36",
        206,
        "es");

    new WiremockHelper(wireMockRule).stubGet(
        "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files/9007c26f-e29d-4924-9c49-8ff064484264",
        206,
        "es");
    //

    getFileWithRange(1, 2);
  }

  @Test
  public void shouldGetFileWithRange2() throws MCSException, IOException {
    //
    new WiremockHelper(wireMockRule).stubGet(
        "mcs/records/7MZWQJF8P84/representations/schema_000001/versions/de084210-a393-11e3-8614-50e549e85271/files/08fcc281-e1fd-4cec-bd33-c12a49145d36",
        206,
        "t_123456789_");
    new WiremockHelper(wireMockRule).stubGet(
        "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files/9007c26f-e29d-4924-9c49-8ff064484264",
        206,
        "t_123456789_");
    //
    getFileWithRange(3, 14);
  }


  @Test
  public void shouldGetFileWithRange3() throws MCSException, IOException {
    //
    new WiremockHelper(wireMockRule).stubGet(
        "mcs/records/7MZWQJF8P84/representations/schema_000001/versions/de084210-a393-11e3-8614-50e549e85271/files/08fcc281-e1fd-4cec-bd33-c12a49145d36",
        206,
        "Test_123456");
    new WiremockHelper(wireMockRule).stubGet(
        "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files/9007c26f-e29d-4924-9c49-8ff064484264",
        206,
        "Test_123456");
    //
    getFileWithRange(0, 10);
  }

  @Test
  public void shouldGetFileWithRange4() throws MCSException, IOException {
    //
    new WiremockHelper(wireMockRule).stubGet(
        "mcs/records/7MZWQJF8P84/representations/schema_000001/versions/de084210-a393-11e3-8614-50e549e85271/files/08fcc281-e1fd-4cec-bd33-c12a49145d36",
        206,
        "T");
    new WiremockHelper(wireMockRule).stubGet(
        "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files/9007c26f-e29d-4924-9c49-8ff064484264",
        206,
        "T");
    //

    getFileWithRange(0, 0);
  }


  private static void getFileWithRange(Integer rangeStart, Integer rangeEnd) throws MCSException, IOException {
    String contentString = "Test_123456789_8";
    byte[] contentBytes = contentString.getBytes(StandardCharsets.UTF_8);
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);
    String range = String.format("bytes=%d-%d", rangeStart, rangeEnd);

    InputStream responseStream = instance.getFile(TEST_CLOUD_ID, TEST_REPRESENTATION_NAME, TEST_VERSION, UPLOADED_FILE_NAME,
        range);

    assertNotNull(responseStream);
    byte[] responseBytes = ByteStreams.toByteArray(responseStream);
    byte[] rangedContentBytes = copyOfRange(contentBytes, rangeStart, rangeEnd + 1);
    assertArrayEquals("Content is incorrect", rangedContentBytes, responseBytes);
  }

  @Test(expected = WrongContentRangeException.class)
  public void shouldThrowWrongContentRangeExceptionForGetFileWithRangeWhenIncorrectFormat() throws MCSException {
    int rangeStart = 1;
    int rangeEnd = 4;

    //
    new WiremockHelper(wireMockRule).stubGet(
        "mcs/records/7MZWQJF8P84/representations/schema_000001/versions/de084210-a393-11e3-8614-50e549e85271/files/08fcc281-e1fd-4cec-bd33-c12a49145d36\"",
        416,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Expected range header format is: bytes=(?&lt;start&gt;\\d+)[-](?&lt;end&gt;\\d*)</details><errorCode>WRONG_CONTENT_RANGE</errorCode></errorInfo>");

    new WiremockHelper(wireMockRule).stubGet(
        "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files/9007c26f-e29d-4924-9c49-8ff064484264",
        416,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Expected range header format is: bytes=(?&lt;start&gt;\\d+)[-](?&lt;end&gt;\\d*)</details><errorCode>WRONG_CONTENT_RANGE</errorCode></errorInfo>");
    //

    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);
    String range = String.format("bytese=%d-%d", rangeStart, rangeEnd);

    instance.getFile(TEST_CLOUD_ID, TEST_REPRESENTATION_NAME, TEST_VERSION, unmovableFileName, range);
  }


  @Test(expected = WrongContentRangeException.class)
  public void shouldThrowWrongContentRangeExceptionForGetFileWithRangeWhenIncorrectRangeValues() throws MCSException {
    int rangeStart = 1;
    int rangeEnd = 50;
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);
    String range = String.format("bytese=%d-%d", rangeStart, rangeEnd);

    //
    new WiremockHelper(wireMockRule).stubGet(
        "/mcs/records/7MZWQJF8P84/representations/schema_000001/versions/de084210-a393-11e3-8614-50e549e85271/files/08fcc281-e1fd-4cec-bd33-c12a49145d36",
        416,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Expected range header format is: bytes=(?&lt;start&gt;\\d+)[-](?&lt;end&gt;\\d*)</details><errorCode>WRONG_CONTENT_RANGE</errorCode></errorInfo>");
    new WiremockHelper(wireMockRule).stubGet(
        "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files/9007c26f-e29d-4924-9c49-8ff064484264",
        416,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Expected range header format is: bytes=(?&lt;start&gt;\\d+)[-](?&lt;end&gt;\\d*)</details><errorCode>WRONG_CONTENT_RANGE</errorCode></errorInfo>");
    //
    instance.getFile(TEST_CLOUD_ID, TEST_REPRESENTATION_NAME, TEST_VERSION, unmovableFileName, range);
  }


  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForGetFileWithoutRange() throws MCSException {
    String incorrectFileName = "edefc11e-1c5f-4a71-adb6-28efdd7b3b00";
    //
    new WiremockHelper(wireMockRule).stubGet(
        "/mcs/records/7MZWQJF8P84/representations/schema_000001/versions/de084210-a393-11e3-8614-50e549e85271/files/edefc11e-1c5f-4a71-adb6-28efdd7b3b00",
        405,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
    //
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    instance.getFile(cloudId, representationName, version, incorrectFileName);
  }


  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowRepresentationNotExistsForGetFileWithoutRangeWhenIncorrectCloudId() throws MCSException {
    String incorrectCloudId = "7MZWQJF8P99";
    //
    new WiremockHelper(wireMockRule).stubGet(
        "/mcs/records/7MZWQJF8P99/representations/schema_000001/versions/de084210-a393-11e3-8614-50e549e85271/files/08fcc281-e1fd-4cec-bd33-c12a49145d36",
        404,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>REPRESENTATION_NOT_EXISTS</errorCode></errorInfo>");
    new WiremockHelper(wireMockRule).stubGet(
        "/mcs/records/7MZWQJF8P99/representations/schema_000001/versions/de084210-a393-11e3-8614-50e549e85271/files/9007c26f-e29d-4924-9c49-8ff064484264",
        405,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
    //

    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    instance.getFile(incorrectCloudId, representationName, version, unmovableFileName);
  }

  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForGetFileWithoutRangeWhenIncorrectRepresentationName()
      throws MCSException {
    String incorrectRepresentationName = "schema_000101";
    //
    new WiremockHelper(wireMockRule).stubGet(
        "/mcs/records/7MZWQJF8P84/representations/schema_000101/versions/de084210-a393-11e3-8614-50e549e85271/files/9007c26f-e29d-4924-9c49-8ff064484264",
        405,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
    //
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    instance.getFile(cloudId, incorrectRepresentationName, version, unmovableFileName);
  }


  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForGetFileWithoutRangeWhenIncorrectVersion()
      throws MCSException {
    String incorrectVersion = "8a64f9b0-98b6-11e3-b072-50e549e85200";
    //
    new WiremockHelper(wireMockRule).stubGet(
        "/mcs/records/7MZWQJF8P84/representations/schema_000001/versions/8a64f9b0-98b6-11e3-b072-50e549e85200/files/9007c26f-e29d-4924-9c49-8ff064484264",
        405,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
    //
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    instance.getFile(cloudId, representationName, incorrectVersion, unmovableFileName);
  }


  @Test
  public void shouldUploadFile() throws MCSException {
    byte[] contentBytes = UPLOADED_FILE_CONTENTS.getBytes(StandardCharsets.UTF_8);
    InputStream contentStream = new ByteArrayInputStream(contentBytes);
    //
    new WiremockHelper(wireMockRule).stubPost(
        "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files",
        201,
        "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files/65d195f0-e2a1-46a1-be8e-d2ba27a12823",
        null);

    new WiremockHelper(wireMockRule).stubPost(
        "/mcs/records/7MZWQJF8P84/representations/schema_000001/versions/de084210-a393-11e3-8614-50e549e85271/files/9cfebb4d-e5d6-4523-9d5b-608d9530ee57",
        202,
        "Test_123456789_");
    //

    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    URI uri = instance.uploadFile(TEST_CLOUD_ID, TEST_REPRESENTATION_NAME, TEST_VERSION, contentStream, mediaType);
    System.out.println(uri);
    assertNotNull(uri);
  }


  @Test(expected = RepresentationNotExistsException.class)
  public void shouldThrowRepresentationNotExistsExceptionForUploadFileWhenIncorrectCloudId() throws MCSException {
    String contentString = "Test_123456789_";
    String incorrectCloudId = "7MZWQJS8P84";
    byte[] contentBytes = contentString.getBytes(StandardCharsets.UTF_8);
    InputStream contentStream = new ByteArrayInputStream(contentBytes);
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    //
    new WiremockHelper(wireMockRule).stubPost(
        "/mcs/records/7MZWQJS8P84/representations/schema_000001/versions/de084210-a393-11e3-8614-50e549e85271/files",
        405,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>REPRESENTATION_NOT_EXISTS</errorCode></errorInfo>");
    //

    instance.uploadFile(incorrectCloudId, representationName, version, contentStream, mediaType);
  }


  @Test(expected = RepresentationNotExistsException.class)
  public void shouldThrowRepresentationNotExistsExceptionForUploadFileWhenIncorrectRepresentationName() throws MCSException {
    String contentString = "Test_123456789_";
    String incorrectRepresentationName = "schema_000101";
    byte[] contentBytes = contentString.getBytes(StandardCharsets.UTF_8);
    InputStream contentStream = new ByteArrayInputStream(contentBytes);
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    //
    new WiremockHelper(wireMockRule).stubPost(
        "/mcs/records/7MZWQJF8P84/representations/schema_000101/versions/de084210-a393-11e3-8614-50e549e85271/files",
        404,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>REPRESENTATION_NOT_EXISTS</errorCode></errorInfo>");
    //

    instance.uploadFile(cloudId, incorrectRepresentationName, version, contentStream, mediaType);
  }


  @Test(expected = RepresentationNotExistsException.class)
  public void shouldThrowRepresentationNotExistsExceptionForUploadFileWhenIncorrectVersion() throws MCSException {
    String contentString = "Test_123456789_";
    String incorrectVersion = "8a64f9b0-98b6-11e3-b072-50e549e85200";
    byte[] contentBytes = contentString.getBytes(StandardCharsets.UTF_8);
    InputStream contentStream = new ByteArrayInputStream(contentBytes);
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    //
    new WiremockHelper(wireMockRule).stubPost(
        "/mcs/records/7MZWQJF8P84/representations/schema_000001/versions/8a64f9b0-98b6-11e3-b072-50e549e85200/files",
        404,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>REPRESENTATION_NOT_EXISTS</errorCode></errorInfo>");
    //

    instance.uploadFile(cloudId, representationName, incorrectVersion, contentStream, mediaType);
  }

  @Test(expected = CannotModifyPersistentRepresentationException.class)
  public void shouldThrowCannotModifyPersistentRepresentationExceptionForUploadFile() throws MCSException {
    String contentString = "Test_123456789_";
    byte[] contentBytes = contentString.getBytes(StandardCharsets.UTF_8);
    InputStream contentStream = new ByteArrayInputStream(contentBytes);
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    //
    new WiremockHelper(wireMockRule).stubPost(
        "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/881c5c00-4259-11e4-9c35-00163eefc9c8/files",
        405,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>CANNOT_MODIFY_PERSISTENT_REPRESENTATION</errorCode></errorInfo>");
    //

    instance.uploadFile(TEST_CLOUD_ID, TEST_REPRESENTATION_NAME, PERSISTED_VERSION, contentStream, mediaType);
  }


  @Test
  public void shouldUploadFileWithChecksum() throws MCSException, NoSuchAlgorithmException {
    String contentString = "Test_123456789_1";
    byte[] contentBytes = contentString.getBytes(StandardCharsets.UTF_8);
    InputStream contentStream = new ByteArrayInputStream(contentBytes);
    String contentChecksum = createMD5(contentBytes);
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    //
    new WiremockHelper(wireMockRule).stubPost(
        "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files",
        201,
        "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files/46688d93-3519-4b4f-b841-639959adf250",
        "\"cc3dedabc38bdafc5a5fd53b5485544f\"",
        null);
    //

    URI uri = instance.uploadFile(TEST_CLOUD_ID, TEST_REPRESENTATION_NAME, TEST_VERSION, contentStream, mediaType,
        contentChecksum);
    assertNotNull(uri);
  }


  @Test(expected = RepresentationNotExistsException.class)
  public void shouldThrowRepresentationNotExistsExceptionForUploadFileWithChecksumWhenIncorrectCloudId()
      throws MCSException, NoSuchAlgorithmException {
    String incorrectCloudId = "7MZWQJF8P00";
    String contentString = "Test_123456789_1";
    byte[] contentBytes = contentString.getBytes(StandardCharsets.UTF_8);
    InputStream contentStream = new ByteArrayInputStream(contentBytes);
    String contentChecksum = createMD5(contentBytes);
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    //
    new WiremockHelper(wireMockRule).stubPost(
        "/mcs/records/7MZWQJF8P00/representations/schema_000001/versions/de084210-a393-11e3-8614-50e549e85271/files",
        404,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>REPRESENTATION_NOT_EXISTS</errorCode></errorInfo>");
    //

    instance.uploadFile(incorrectCloudId, representationName, version, contentStream, mediaType, contentChecksum);
  }


  @Test(expected = RepresentationNotExistsException.class)
  public void shouldThrowRepresentationNotExistsExceptionForUploadFileWithChecksumWhenIncorrectRepresentationName()
      throws MCSException, NoSuchAlgorithmException {
    String incorrectRepresentationName = "schema_000101";
    String contentString = "Test_123456789_1";
    byte[] contentBytes = contentString.getBytes(StandardCharsets.UTF_8);
    InputStream contentStream = new ByteArrayInputStream(contentBytes);
    String contentChecksum = createMD5(contentBytes);
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    //
    new WiremockHelper(wireMockRule).stubPost(
        "/mcs/records/7MZWQJF8P84/representations/schema_000101/versions/de084210-a393-11e3-8614-50e549e85271/files",
        404,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>REPRESENTATION_NOT_EXISTS</errorCode></errorInfo>");
    //

    instance.uploadFile(cloudId, incorrectRepresentationName, version, contentStream, mediaType, contentChecksum);
  }


  @Test(expected = RepresentationNotExistsException.class)
  public void shouldThrowRepresentationNotExistsExceptionForUploadFileWithChecksumWhenIncorrectVersion()
      throws MCSException, NoSuchAlgorithmException {
    String incorrectVersion = "8a64f9b0-98b6-11e3-b072-50e549e85200";
    String contentString = "Test_123456789_1";
    byte[] contentBytes = contentString.getBytes(StandardCharsets.UTF_8);
    InputStream contentStream = new ByteArrayInputStream(contentBytes);
    String contentChecksum = createMD5(contentBytes);
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    //
    new WiremockHelper(wireMockRule).stubPost(
        "/mcs/records/7MZWQJF8P84/representations/schema_000001/versions/8a64f9b0-98b6-11e3-b072-50e549e85200/files",
        404,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>REPRESENTATION_NOT_EXISTS</errorCode></errorInfo>");
    //

    instance.uploadFile(cloudId, representationName, incorrectVersion, contentStream, mediaType, contentChecksum);
  }


  @Test(expected = CannotModifyPersistentRepresentationException.class)
  public void shouldThrowCannotModifyPersistentRepresentationExceptionForUploadFileWithChecksum()
      throws MCSException, NoSuchAlgorithmException {
    String contentString = "Test_123456789_1";
    byte[] contentBytes = contentString.getBytes(StandardCharsets.UTF_8);
    InputStream contentStream = new ByteArrayInputStream(contentBytes);
    String contentChecksum = createMD5(contentBytes);
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    //
    new WiremockHelper(wireMockRule).stubPost(
        "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/881c5c00-4259-11e4-9c35-00163eefc9c8/files",
        405,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>CANNOT_MODIFY_PERSISTENT_REPRESENTATION</errorCode></errorInfo>");
    //

    instance.uploadFile(TEST_CLOUD_ID, TEST_REPRESENTATION_NAME, PERSISTED_VERSION, contentStream, mediaType, contentChecksum);
  }


  @Test(expected = MCSException.class)
  public void shouldThrowMCSExceptionForUploadFile() throws MCSException, NoSuchAlgorithmException {
    String contentString = "Test_123456789_1";
    byte[] contentBytes = contentString.getBytes(StandardCharsets.UTF_8);
    InputStream contentStream = new ByteArrayInputStream(contentBytes);
    String contentChecksum = createMD5(contentBytes);
    String incorrectContentChecksum = contentChecksum.substring(1) + "0";
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    //
    new WiremockHelper(wireMockRule).stubPost(
        "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files",
        201,
        "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files/26bbc4ac-5da6-4736-b537-9ccb0d35125d",
        "\"cc3dedabc38bdafc5a5fd53b5485544f\"",
        null);
    //

    instance.uploadFile(TEST_CLOUD_ID, TEST_REPRESENTATION_NAME, TEST_VERSION, contentStream, mediaType,
        incorrectContentChecksum);
  }


  @Test
  public void shouldModifyFile() throws MCSException, NoSuchAlgorithmException {
    byte[] contentBytes = MODIFIED_FILE_CONTENTS.getBytes(StandardCharsets.UTF_8);
    InputStream contentStream = new ByteArrayInputStream(contentBytes);
    String contentChecksum = createMD5(contentBytes);

    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    //
    new WiremockHelper(wireMockRule).stubPut(
        "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files/06abeac8-6221-4399-be68-5be5ae8d1473",
        204,
        "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files/06abeac8-6221-4399-be68-5be5ae8d1473",
        "\"e0b2ac158446e3169a8ca9e9d084bd42\"",
        null);
    //

    URI uri = instance.modifyFile(TEST_CLOUD_ID, TEST_REPRESENTATION_NAME, TEST_VERSION, contentStream, mediaType,
        MODIFIED_FILE_NAME, contentChecksum);

    assertNotNull(uri);
  }


  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForModifyFileWhenIncorrectCloudId()
      throws MCSException, NoSuchAlgorithmException {
    String incorrectCloudId = "12c068c9-461d-484e-878f-099c5fca4400";
    String contentString = "Test_123456789_123456";
    byte[] contentBytes = contentString.getBytes(StandardCharsets.UTF_8);
    InputStream contentStream = new ByteArrayInputStream(contentBytes);
    String contentChecksum = createMD5(contentBytes);
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    //
    new WiremockHelper(wireMockRule).stubPut(
        "/mcs/records/12c068c9-461d-484e-878f-099c5fca4400/representations/schema_000001/versions/de084210-a393-11e3-8614-50e549e85271/files/12c068c9-461d-484e-878f-099c5fca447f",
        405,
        "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files/06abeac8-6221-4399-be68-5be5ae8d1473",
        "\"e0b2ac158446e3169a8ca9e9d084bd42\"",
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
    //

    instance.modifyFile(incorrectCloudId, representationName, version, contentStream, mediaType, modyfiedFileName,
        contentChecksum);
  }


  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForModifyFileWhenIncorrectRepresentationName()
      throws MCSException, NoSuchAlgorithmException {
    String incorrectRepresentationName = "schema_000101";
    String contentString = "Test_123456789_123456";
    byte[] contentBytes = contentString.getBytes(StandardCharsets.UTF_8);
    InputStream contentStream = new ByteArrayInputStream(contentBytes);
    String contentChecksum = createMD5(contentBytes);
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    //
    new WiremockHelper(wireMockRule).stubPut(
        "/mcs/records/7MZWQJF8P84/representations/schema_000101/versions/de084210-a393-11e3-8614-50e549e85271/files/12c068c9-461d-484e-878f-099c5fca447f",
        405,
        "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files/06abeac8-6221-4399-be68-5be5ae8d1473",
        "\"e0b2ac158446e3169a8ca9e9d084bd42\"",
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
    //

    instance.modifyFile(cloudId, incorrectRepresentationName, version, contentStream, mediaType, modyfiedFileName,
        contentChecksum);
  }


  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForModifyFileWhenIncorrectVersion()
      throws MCSException, NoSuchAlgorithmException {
    String incorrectVersion = "8a64f9b0-98b6-11e3-b072-50e549e85200";
    String contentString = "Test_123456789_123456";
    byte[] contentBytes = contentString.getBytes(StandardCharsets.UTF_8);
    InputStream contentStream = new ByteArrayInputStream(contentBytes);
    String contentChecksum = createMD5(contentBytes);
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    //
    new WiremockHelper(wireMockRule).stubPut(
        "/mcs/records/7MZWQJF8P84/representations/schema_000001/versions/8a64f9b0-98b6-11e3-b072-50e549e85200/files/12c068c9-461d-484e-878f-099c5fca447f",
        405,
        "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files/06abeac8-6221-4399-be68-5be5ae8d1473",
        "\"e0b2ac158446e3169a8ca9e9d084bd42\"",
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
    //

    instance.modifyFile(cloudId, representationName, incorrectVersion, contentStream, mediaType, modyfiedFileName,
        contentChecksum);
  }

  @Test(expected = MCSException.class)
  public void shouldThrowMCSExceptionForModifyFile() throws MCSException, NoSuchAlgorithmException {
    String contentString = "Test_123456789_123456";
    byte[] contentBytes = contentString.getBytes(StandardCharsets.UTF_8);
    InputStream contentStream = new ByteArrayInputStream(contentBytes);
    String contentChecksum = createMD5(contentBytes);
    String incorrectContentChecksum = contentChecksum.substring(1) + "0";
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    //
    new WiremockHelper(wireMockRule).stubPut(
        "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files/06abeac8-6221-4399-be68-5be5ae8d1473",
        204,
        "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files/06abeac8-6221-4399-be68-5be5ae8d1473",
        "\"e0b2ac158446e3169a8ca9e9d084bd42\"",
        null);
    //

    instance.modifyFile(TEST_CLOUD_ID, TEST_REPRESENTATION_NAME, TEST_VERSION, contentStream, mediaType, MODIFIED_FILE_NAME,
        incorrectContentChecksum);
  }


  @Test(expected = FileNotExistsException.class)
  public void shouldDeleteFile() throws MCSException {
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    //
    new WiremockHelper(wireMockRule).stubDelete(
        "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files/Test_123456789_",
        204);
    new WiremockHelper(wireMockRule).stubGet(
        "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/e91d6300-431c-11e4-8576-00163eefc9c8/files/Test_123456789_",
        404,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>FILE_NOT_EXISTS</errorCode></errorInfo>");
    //

    instance.deleteFile(TEST_CLOUD_ID, TEST_REPRESENTATION_NAME, TEST_VERSION, UPLOADED_FILE_CONTENTS);
    instance.getFile(TEST_CLOUD_ID, TEST_REPRESENTATION_NAME, TEST_VERSION, UPLOADED_FILE_CONTENTS);

  }


  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForDeleteFileWhenIncorrectCloudId() throws MCSException {
    String incorrectCloudId = "7MZWQJF8P99";
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    //
    new WiremockHelper(wireMockRule).stubDelete(
        "/mcs/records/7MZWQJF8P99/representations/schema_000001/versions/de084210-a393-11e3-8614-50e549e85271/files/d64b423b-1018-4526-ab4b-3539261ff067",
        405,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
    //

    instance.deleteFile(incorrectCloudId, representationName, version, deletedFileName);
  }


  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForDeleteFileWhenIncorrectRepresentationName()
      throws MCSException {
    String incorrectRepresentationName = "schema_000101";
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    //
    new WiremockHelper(wireMockRule).stubDelete(
        "/mcs/records/7MZWQJF8P84/representations/schema_000101/versions/de084210-a393-11e3-8614-50e549e85271/files/d64b423b-1018-4526-ab4b-3539261ff067",
        405,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
    //

    instance.deleteFile(cloudId, incorrectRepresentationName, version, deletedFileName);
  }


  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForDeleteFileWhenIncorrectVersion() throws MCSException {
    String incorrectVersion = "8a64f9b0-98b6-11e3-b072-50e549e85200";
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);
    //
    new WiremockHelper(wireMockRule).stubDelete(
        "/mcs/records/7MZWQJF8P84/representations/schema_000001/versions/8a64f9b0-98b6-11e3-b072-50e549e85200/files/d64b423b-1018-4526-ab4b-3539261ff067",
        405,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
    //
    instance.deleteFile(cloudId, representationName, incorrectVersion, deletedFileName);
  }


  @Test(expected = AccessDeniedOrObjectDoesNotExistException.class)
  public void shouldThrowAccessDeniedOrObjectDoesNotExistExceptionForDeleteFile() throws MCSException {
    String notExistDeletedFileName = "d64b423b-1018-4526-ab4b-3539261ff000";
    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    //
    new WiremockHelper(wireMockRule).stubDelete(
        "/mcs/records/7MZWQJF8P84/representations/schema_000001/versions/de084210-a393-11e3-8614-50e549e85271/files/d64b423b-1018-4526-ab4b-3539261ff000",
        405,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><details>Access is denied</details><errorCode>ACCESS_DENIED_OR_OBJECT_DOES_NOT_EXIST_EXCEPTION</errorCode></errorInfo>");
    //

    instance.deleteFile(cloudId, representationName, version, notExistDeletedFileName);
  }


  @Test(expected = CannotModifyPersistentRepresentationException.class)
  public void shouldThrowCannotModifyPersistentRepresentationExceptionForDeleteFile() throws MCSException {

    FileServiceClient instance = new FileServiceClient(baseUrl, username, password);

    //
    new WiremockHelper(wireMockRule).stubDelete(
        "/mcs/records/W3KBLNZDKNQ/representations/schema66/versions/eb5c0a60-4306-11e4-8576-00163eefc9c8/files/b32b56e9-94d7-44b8-9010-7a1795ee7f95",
        405,
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><errorInfo><errorCode>CANNOT_MODIFY_PERSISTENT_REPRESENTATION</errorCode></errorInfo>");
    //

    instance.deleteFile(TEST_CLOUD_ID, TEST_REPRESENTATION_NAME, "eb5c0a60-4306-11e4-8576-00163eefc9c8", DELETED_FILE_NAME);
  }

  private String createMD5(byte[] bytes) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("MD5");
    md.update(bytes);
    return DatatypeConverter.printHexBinary(md.digest()).toLowerCase();
  }
}
