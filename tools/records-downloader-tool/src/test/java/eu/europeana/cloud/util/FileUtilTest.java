package eu.europeana.cloud.util;

import org.apache.commons.io.FilenameUtils;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.assertEquals;

/**
 * Created by Tarek on 9/15/2016.
 */
public class FileUtilTest {

  private static final String FOLDER_PATH = "folder/";
  private static final String EXTENSION = ".txt";
  private static final String FILE_NAME_WITH_EXTENSION = "FILENAME" + EXTENSION;
  private static final String FILE_NAME_WITHOUT_EXTENSION = "FILENAME";
  private static final String FILE_PATH = FOLDER_PATH + FILE_NAME_WITH_EXTENSION;
  private final String ZIP_EXTENSION = "zip";
  private static final String ECLOUD_SUFFIX = "ecloud-dataset";

  @Test
  public void shouldCreateTheCorrectFilePath() throws Exception {
    String filePath = FileUtil.createFilePath(FOLDER_PATH, FILE_NAME_WITHOUT_EXTENSION, EXTENSION);
    assertEquals(filePath, FILE_PATH);
    filePath = FileUtil.createFilePath(FOLDER_PATH, FILE_NAME_WITH_EXTENSION, EXTENSION);
    assertEquals(filePath, FILE_PATH);

  }

  @Test
  public void testCreateZipFolderPath() {
    Date date = new Date();
    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss-ssss");
    String expectedFolderName = ECLOUD_SUFFIX + "-" + dateFormat.format(date);
    String folderPath = FileUtil.createZipFolderPath(date);
    String extension = FilenameUtils.getExtension(folderPath);
    String folderName = FilenameUtils.getBaseName(folderPath);
    assertEquals(extension, ZIP_EXTENSION);
    assertEquals(folderName, expectedFolderName);


  }
}
