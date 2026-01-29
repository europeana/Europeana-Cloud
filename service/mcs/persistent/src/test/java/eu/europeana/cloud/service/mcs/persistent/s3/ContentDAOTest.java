package eu.europeana.cloud.service.mcs.persistent.s3;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.test.S3TestHelper;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * @author krystian.
 */
public abstract class ContentDAOTest {
  private final static String EXAMPLE_FILE_CONTENT = "This is a test content";
  // md5 generated via usage of messageDigest with md5 algorithm
  private final static String EXAMPLE_MD5 = "75e6f8645a9f5059e0970f95a3a0c0be";

  @Autowired
  protected ContentDAO instance;

  @BeforeClass
  public static void setUp() {
    S3TestHelper.startS3MockServer();
  }

  @Before
  public void cleanUpBeforeTest() {
    S3TestHelper.cleanUpBetweenTests();
  }
  @AfterClass
  public static void cleanUp() {
    S3TestHelper.stopS3MockServer();
  }
  @Test
  public void shouldPutAndGetContent()
      throws Exception {
    String fileName = "someFileName";
    byte[] content = EXAMPLE_FILE_CONTENT.getBytes(StandardCharsets.UTF_8);
    InputStream is = new ByteArrayInputStream(content);

    File file = new File();
    PutResult result = instance.putContent(fileName, is);
    String md5 = result.getMd5();
    file.setMd5(md5);
    file.setContentLength(result.getContentLength());
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    instance.getContent(md5, fileName, -1, -1, os);
    assertArrayEquals(content, os.toByteArray());
    assertEquals(file.getMd5(), EXAMPLE_MD5);
    assertEquals(file.getContentLength(), content.length);
    String md5Hex = DigestUtils.md5Hex(content);
    //check if file md5 got updated
    assertNotNull(file.getMd5());
    //check if md5 in file is correct
    assertEquals(file.getMd5(), md5Hex);
    //check if md5 in file is correct
    assertEquals(DigestUtils.md5Hex(os.toByteArray()), md5Hex);
  }

  @Test
  public void shouldRetrieveRangeOfBytes()
      throws Exception {
    String fileName = "rangeFile";
    InputStream is = new ByteArrayInputStream(EXAMPLE_FILE_CONTENT.getBytes(StandardCharsets.UTF_8));

    File file = new File();
    PutResult result = instance.putContent(fileName, is);
    String md5 = result.getMd5();
    file.setMd5(md5);
    file.setContentLength(result.getContentLength());

    byte[] contentBytes = EXAMPLE_FILE_CONTENT.getBytes();

    int from = -1;
    int to = -1;
    checkRange(from, to, contentBytes, fileName, md5);

    from = -1;
    to = 3;
    checkRange(from, to, contentBytes, fileName, md5);

    from = 3;
    to = -1;
    checkRange(from, to, contentBytes, fileName, md5);

    from = 2;
    to = 2;
    checkRange(from, to, contentBytes, fileName, md5);

    from = 3;
    to = 6;
    checkRange(from, to, contentBytes, fileName, md5);

    assertEquals(file.getMd5(), EXAMPLE_MD5);
  }

  private void checkRange(int from, int to, byte[] expected, String fileName, String md5)
      throws Exception {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    instance.getContent(md5, fileName, from, to, os);

    int rangeStart = from;
    int rangeEnd = to + 1;
    if (from == -1) {
      rangeStart = 0;
    }
    if (to == -1) {
      rangeEnd = expected.length;
    }
    byte[] rangeOfContent = Arrays.copyOfRange(expected, rangeStart, rangeEnd);
    assertTrue(String.format("Ranges not equal %d-%d", from, to), Arrays.equals(rangeOfContent, os.toByteArray()));
  }

  @Test(expected = FileNotExistsException.class)
  public void testDeleteContent()
      throws Exception {
    String objectId = "to_delete";
    File file = new File();
    InputStream is = new ByteArrayInputStream(EXAMPLE_FILE_CONTENT.getBytes());
    PutResult result = instance.putContent(objectId, is);
    String md5 = result.getMd5();
    file.setMd5(md5);
    file.setContentLength(result.getContentLength());

    assertEquals(file.getMd5(), EXAMPLE_MD5);
    instance.deleteContent(md5, objectId);
    instance.getContent(md5, objectId, -1, -1, null);
  }

  @Test(expected = FileNotExistsException.class)
  public void shouldThrowNotFoundExpWhenGettingNotExistingFile()
      throws Exception {
    String objectId = "not_exist";
    instance.getContent(EXAMPLE_MD5, objectId, -1, -1, null);
  }

  @Test(expected = FileNotExistsException.class)
  public void shouldThrowNotFoundExpWhenDeletingNotExistingFile()
      throws Exception {
    String objectId = "not_exist";
    instance.deleteContent(EXAMPLE_MD5, objectId);
  }


  @Test
  public void shouldCopyContent()
          throws Exception {
    String sourceObjectId = "sourceObjectId";
    String trgObjectId = "trgObjectId";
    String content = "This is a test content";
    InputStream is = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

    File file = new File();
    //input source object
    PutResult putResult = instance.putContent(sourceObjectId, is);
    String md5 = putResult.getMd5();
    file.setMd5(md5);
    file.setContentLength(putResult.getContentLength());
    //copy object
    instance.copyContent(md5, sourceObjectId, trgObjectId);

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    instance.getContent(md5, trgObjectId, -1, -1, os);
    String result = os.toString(StandardCharsets.UTF_8);
    assertEquals(file.getMd5(), EXAMPLE_MD5);
    assertEquals(content, result);
  }
}
