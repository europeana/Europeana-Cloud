package eu.europeana.cloud.service.commons.md5;


import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class FileMd5GenerationServiceTest {

  @Test
  void shouldGenerateDifferentMd5ForSlightlyDifferentFiles() throws URISyntaxException, IOException {
    UUID record1 = FileMd5GenerationService.generateUUID(Paths.get(ClassLoader.getSystemResource("Lithuania_1.xml").toURI()));
    UUID record2 = FileMd5GenerationService.generateUUID(Paths.get(ClassLoader.getSystemResource("Lithuania_1_1.xml").toURI()));
    assertNotSame(record1, record2);
  }

  @Test
  void shouldGenerateDifferentMd5ForSlightlyDifferentFilesProvidedAsByteArrays() throws IOException {
    UUID record1 = FileMd5GenerationService.generateUUID(
            FileUtils.readFileToByteArray(new File(ClassLoader.getSystemResource("Lithuania_1.xml").getFile())));
    UUID record2 = FileMd5GenerationService.generateUUID(
            FileUtils.readFileToByteArray(new File(ClassLoader.getSystemResource("Lithuania_1_1.xml").getFile())));
    assertNotSame(record1, record2);
  }

  @Test
  void shouldGenerateTheSameMd5ForSameFile() throws URISyntaxException, IOException {
    UUID record1 = FileMd5GenerationService.generateUUID(Paths.get(ClassLoader.getSystemResource("Lithuania_1.xml").toURI()));
    UUID record2 = FileMd5GenerationService.generateUUID(
            Paths.get(getClass().getClassLoader().getResource("Lithuania_1.xml").toURI()));
    assertEquals(record1, record2);
  }

  @Test
  void shouldGenerateTheSameMd5ForSameFileProvidedAsByteArray() throws IOException {
    UUID record1 = FileMd5GenerationService.generateUUID(
            FileUtils.readFileToByteArray(new File(ClassLoader.getSystemResource("Lithuania_1.xml").getFile())));
    UUID record2 = FileMd5GenerationService.generateUUID(
            FileUtils.readFileToByteArray(new File(ClassLoader.getSystemResource("Lithuania_1.xml").getFile())));
    assertEquals(record1, record2);
  }

  @Test
  void shouldGenerateDifferentMd5ForDifferentFiles() throws URISyntaxException, IOException {
    UUID record1 = FileMd5GenerationService.generateUUID(Paths.get(ClassLoader.getSystemResource("Lithuania_1.xml").toURI()));
    UUID record1_1 = FileMd5GenerationService.generateUUID(Paths.get(ClassLoader.getSystemResource("Lithuania_1_1.xml").toURI()));
    UUID record2 = FileMd5GenerationService.generateUUID(Paths.get(ClassLoader.getSystemResource("Lithuania_2.xml").toURI()));
    UUID record3 = FileMd5GenerationService.generateUUID(Paths.get(ClassLoader.getSystemResource("Lithuania_3.xml").toURI()));
    UUID record4 = FileMd5GenerationService.generateUUID(Paths.get(ClassLoader.getSystemResource("Lithuania_4.xml").toURI()));

    assertNotEquals(record1, record1_1, record2, record3, record4);
  }

  @Test
  void shouldGenerateDifferentMd5ForDifferentFilesProvidedAsByteArrays() throws IOException {
    UUID record1 = FileMd5GenerationService.generateUUID(
            FileUtils.readFileToByteArray(new File(ClassLoader.getSystemResource("Lithuania_1.xml").getFile())));
    UUID record1_1 = FileMd5GenerationService.generateUUID(
            FileUtils.readFileToByteArray(new File(ClassLoader.getSystemResource("Lithuania_1_1.xml").getFile())));
    UUID record2 = FileMd5GenerationService.generateUUID(
            FileUtils.readFileToByteArray(new File(ClassLoader.getSystemResource("Lithuania_2.xml").getFile())));
    UUID record3 = FileMd5GenerationService.generateUUID(
            FileUtils.readFileToByteArray(new File(ClassLoader.getSystemResource("Lithuania_3.xml").getFile())));
    UUID record4 = FileMd5GenerationService.generateUUID(
            FileUtils.readFileToByteArray(new File(ClassLoader.getSystemResource("Lithuania_4.xml").getFile())));

    assertNotEquals(record1, record1_1, record2, record3, record4);
  }


  @Test
  void shouldGenerateTheSameUUIDsAsEarlier() throws IOException {
    assertEquals(UUID.fromString("de709ba0-6d52-31e5-a7c1-24793dca071d"), FileMd5GenerationService.generateUUID(
            FileUtils.readFileToByteArray(new File(ClassLoader.getSystemResource("Lithuania_1.xml").getFile()))));
    assertEquals(UUID.fromString("7d13c967-fcb9-3dbf-8243-0ae4c03bb299"), FileMd5GenerationService.generateUUID(
            FileUtils.readFileToByteArray(new File(ClassLoader.getSystemResource("Lithuania_1_1.xml").getFile()))));
    assertEquals(UUID.fromString("75b85fde-42be-3cd0-87d6-693485ec3124"), FileMd5GenerationService.generateUUID(
            FileUtils.readFileToByteArray(new File(ClassLoader.getSystemResource("Lithuania_2.xml").getFile()))));
    assertEquals(UUID.fromString("79071711-1865-3d60-b872-665be230000d"), FileMd5GenerationService.generateUUID(
            FileUtils.readFileToByteArray(new File(ClassLoader.getSystemResource("Lithuania_3.xml").getFile()))));
    assertEquals(UUID.fromString("e3d4dd38-9b62-39a6-b03d-a86e54ef64f2"), FileMd5GenerationService.generateUUID(
            FileUtils.readFileToByteArray(new File(ClassLoader.getSystemResource("Lithuania_4.xml").getFile()))));

  }

  @Test
  void shouldGenerateThisSameUUIDDirectlyAndViaMD5Path() throws URISyntaxException, IOException {
    testUIIDGenerationDirectlyAndByServiceFilePath("Lithuania_1.xml");
    testUIIDGenerationDirectlyAndByServiceFilePath("Lithuania_1_1.xml");
    testUIIDGenerationDirectlyAndByServiceFilePath("Lithuania_2.xml");
    testUIIDGenerationDirectlyAndByServiceFilePath("Lithuania_3.xml");
    testUIIDGenerationDirectlyAndByServiceFilePath("Lithuania_4.xml");
  }

  @Test
  void shouldGenerateThisSameUUIDDirectlyAndViaMD5FileData() throws URISyntaxException, IOException {
    testUIIDGenerationDirectlyAndByServiceFileData("Lithuania_1.xml");
    testUIIDGenerationDirectlyAndByServiceFileData("Lithuania_1_1.xml");
    testUIIDGenerationDirectlyAndByServiceFileData("Lithuania_2.xml");
    testUIIDGenerationDirectlyAndByServiceFileData("Lithuania_3.xml");
    testUIIDGenerationDirectlyAndByServiceFileData("Lithuania_4.xml");
  }

  @Test
  void shouldGenerateEqualMd5ForApacheCommonsAndJavaDefault() throws URISyntaxException, IOException {
    testDefaultJavaMD5VsApacheCommonsMD5("Lithuania_1.xml");
    testDefaultJavaMD5VsApacheCommonsMD5("Lithuania_1_1.xml");
    testDefaultJavaMD5VsApacheCommonsMD5("Lithuania_2.xml");
    testDefaultJavaMD5VsApacheCommonsMD5("Lithuania_3.xml");
    testDefaultJavaMD5VsApacheCommonsMD5("Lithuania_4.xml");
  }

  @Test
  void shouldMD5GeneratedAsApacheCommonsHexBeEqualsToUUIDGeneratedDirectly() throws URISyntaxException, IOException {
    testMD5GeneratedAsHexAndDirectlyAsUUIDIsEquals("Lithuania_1.xml");
    testMD5GeneratedAsHexAndDirectlyAsUUIDIsEquals("Lithuania_1_1.xml");
    testMD5GeneratedAsHexAndDirectlyAsUUIDIsEquals("Lithuania_2.xml");
    testMD5GeneratedAsHexAndDirectlyAsUUIDIsEquals("Lithuania_3.xml");
    testMD5GeneratedAsHexAndDirectlyAsUUIDIsEquals("Lithuania_4.xml");
  }

  @Test
  void shouldDetectNullForBytes() {
    assertThrows(NullPointerException.class, () -> FileMd5GenerationService.md5ToUUID((byte[]) null));
  }

  @Test
  void shouldDetectNullForString() {
    assertThrows(NullPointerException.class, () -> FileMd5GenerationService.md5ToUUID((String) null));
  }

  @Test
  void shouldDetectBadLengthOfArray() {
    assertThrows(IllegalArgumentException.class, () -> FileMd5GenerationService.md5ToUUID(new byte[10]));
  }

  @Test
  void shouldDetectBadLengthOfString() {
    assertThrows(IllegalArgumentException.class, () -> FileMd5GenerationService.md5ToUUID(new String(new byte[8])));
  }

  @Test
  void shouldDetectNotValidHexString() {
    assertThrows(IllegalArgumentException.class, () -> FileMd5GenerationService.md5ToUUID("000102030405060708090a0b0c0d0eZZ"));
  }


  private void testUIIDGenerationDirectlyAndByServiceFilePath(String filename) throws URISyntaxException, IOException {
    Path pathToFile = Paths.get(ClassLoader.getSystemResource(filename).toURI());

    byte[] md5 = generateMd5(pathToFile);
    UUID uuidConvertedFromMd5 = FileMd5GenerationService.md5ToUUID(md5);

    UUID uuidDirect = FileMd5GenerationService.generateUUID(pathToFile);

    assertEquals(uuidDirect, uuidConvertedFromMd5);
  }

  private void testUIIDGenerationDirectlyAndByServiceFileData(String filename) throws URISyntaxException, IOException {
    byte[] fileData = Files.readAllBytes(Paths.get(ClassLoader.getSystemResource(filename).toURI()));

    byte[] md5 = generateMd5(fileData);
    UUID uuidConvertedFromMd5 = FileMd5GenerationService.md5ToUUID(md5);

    UUID uuidDirect = FileMd5GenerationService.generateUUID(fileData);

    assertEquals(uuidDirect, uuidConvertedFromMd5);
  }

  private void testDefaultJavaMD5VsApacheCommonsMD5(String filename) throws URISyntaxException, IOException {
    Path pathToFile = Paths.get(ClassLoader.getSystemResource(filename).toURI());

    byte[] md5FromApacheCommons = org.apache.commons.codec.digest.DigestUtils.md5(Files.newInputStream(pathToFile));
    byte[] ms5FromDefaultJava = generateMd5(Files.readAllBytes(pathToFile));

    assertArrayEquals(md5FromApacheCommons, ms5FromDefaultJava);
  }

  private void testMD5GeneratedAsHexAndDirectlyAsUUIDIsEquals(String filename) throws URISyntaxException, IOException {
    Path pathToFile = Paths.get(ClassLoader.getSystemResource(filename).toURI());

    String md5Hex = org.apache.commons.codec.digest.DigestUtils.md5Hex(Files.newInputStream(pathToFile));
    UUID md5UUIDFromDefaultJava = FileMd5GenerationService.generateUUID(Files.readAllBytes(pathToFile));

    assertEquals(FileMd5GenerationService.md5ToUUID(md5Hex), md5UUIDFromDefaultJava);
  }

  private void assertNotEquals(UUID... uuids) {
    assertEquals(uuids.length, Sets.newHashSet(uuids).size());
  }

  public static byte[] generateMd5(Path filePath) throws IOException {
    return generateMd5(Files.readAllBytes(filePath));
  }

  public static byte[] generateMd5(byte[] fileBytes) {
    try {
      return MessageDigest.getInstance("MD5").digest(fileBytes);
    } catch (NoSuchAlgorithmException var3) {
      throw new InternalError("MD5 not supported", var3);
    }
  }


}