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

  @Autowired
  protected ContentDAO instance;

  @BeforeClass
  public static void setUp(){
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
    byte[] content = ("This is a test content").getBytes(StandardCharsets.UTF_8);
    InputStream is = new ByteArrayInputStream(content);

    File file = new File();
    PutResult result = instance.putContent(fileName, is);
    file.setMd5(result.getMd5());
    file.setContentLength(result.getContentLength());
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    instance.getContent(fileName, -1, -1, os);
    assertArrayEquals(content, os.toByteArray());

    assertEquals(file.getContentLength(), content.length);
    String md5Hex = DigestUtils.md5Hex(content);
    //check if file md5 got updated
    assertNotNull(file.getMd5());
    //check if md5 in file is correct
    assertEquals(file.getMd5(), md5Hex);
    //check if md5 in file is correct
    assertEquals(md5Hex, DigestUtils.md5Hex(os.toByteArray()));
  }

  @Test
  public void shouldRetrieveRangeOfBytes()
      throws Exception {
    String fileName = "rangeFile";
    String content = "This is a test content";
    InputStream is = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

    File file = new File();
    PutResult result = instance.putContent(fileName, is);
    file.setMd5(result.getMd5());
    file.setContentLength(result.getContentLength());

    int from = -1;
    int to = -1;
    checkRange(from, to, content.getBytes(), fileName);

    from = -1;
    to = 3;
    checkRange(from, to, content.getBytes(), fileName);

    from = 3;
    to = -1;
    checkRange(from, to, content.getBytes(), fileName);

    from = 2;
    to = 2;
    checkRange(from, to, content.getBytes(), fileName);

    from = 3;
    to = 6;
    checkRange(from, to, content.getBytes(), fileName);
  }

  private void checkRange(int from, int to, byte[] expected, String fileName)
      throws Exception {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    instance.getContent(fileName, from, to, os);

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
    String content = "This is a test content";
    InputStream is = new ByteArrayInputStream(content.getBytes());
    PutResult result = instance.putContent(objectId, is);
    file.setMd5(result.getMd5());
    file.setContentLength(result.getContentLength());

    instance.deleteContent(objectId);
    instance.getContent(objectId, -1, -1, null);
  }

  @Test(expected = FileNotExistsException.class)
  public void shouldThrowNotFoundExpWhenGettingNotExistingFile()
      throws Exception {
    String objectId = "not_exist";
    instance.getContent(objectId, -1, -1, null);
  }

  @Test(expected = FileNotExistsException.class)
  public void shouldThrowNotFoundExpWhenDeletingNotExistingFile()
      throws Exception {
    String objectId = "not_exist";
    instance.deleteContent(objectId);
  }

  @Test(expected = FileNotExistsException.class)
  public void shouldThrowNotFoundExpWhenCopingNotExistingFile()
      throws Exception {
    String objectId = "not_exist";
    String trg = "trg_name";
    instance.copyContent(objectId, trg);
  }

  @Test(expected = FileAlreadyExistsException.class)
  public void shouldThrowAlreadyExpWhenCopingToExistingFile()
      throws Exception {
    String sourceObjectId = "srcObjId";
    String trgObjectId = "trgObjId";
    String content = "This is a test content";
    InputStream is = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    instance.putContent(sourceObjectId, is);
    is = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    instance.putContent(trgObjectId, is);

    instance.copyContent(sourceObjectId, trgObjectId);
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
    file.setMd5(putResult.getMd5());
    file.setContentLength(putResult.getContentLength());
    //copy object
    instance.copyContent(sourceObjectId, trgObjectId);

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    instance.getContent(trgObjectId, -1, -1, os);
    String result = os.toString(StandardCharsets.UTF_8);
    assertEquals(content, result);
  }
}
