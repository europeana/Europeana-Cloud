package eu.europeana.cloud.http.service;

import eu.europeana.cloud.http.exceptions.CompressionExtensionNotRecognizedException;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


public class GzUnpackingServiceTest {

  private FileUnpackingService gzUnpackingService;
  private final static String DESTINATION_DIR = "src/test/resources/__files/";
  private final static int XML_FILES_COUNT = 13;
  private final static String FILE_NAME = "gzFile";
  private final static String FILE_NAME2 = "gzFileWithCompressedGZFiles";
  private final static String FILE_NAME3 = "gzFilesWithMixedCompressedFiles";
  private final static String XML_TYPE = "xml";

  @BeforeEach
  void init() {
    gzUnpackingService = new GzUnpackingService();
  }

  @Test
  void shouldUnpackTheTarGzFilesRecursively() throws CompressionExtensionNotRecognizedException, IOException {
    gzUnpackingService.unpackFile(DESTINATION_DIR + FILE_NAME + ".tar.gz", DESTINATION_DIR);
    Collection files = getXMLFiles(DESTINATION_DIR + FILE_NAME);
    assertNotNull(files);
    assertEquals(XML_FILES_COUNT, files.size());
  }

  @Test
  void shouldUnpackTheTarGzFilesRecursivelyWithCompressedXMLFiles()
          throws CompressionExtensionNotRecognizedException, IOException {
    gzUnpackingService.unpackFile(DESTINATION_DIR + FILE_NAME2 + ".tar.gz", DESTINATION_DIR);
    Collection files = getXMLFiles(DESTINATION_DIR + FILE_NAME2);
    assertNotNull(files);
    assertEquals(XML_FILES_COUNT, files.size());
  }

  @Test
  void shouldUnpackTheTGZFilesRecursivelyWithCompressedXMLFiles()
          throws CompressionExtensionNotRecognizedException, IOException {
    gzUnpackingService.unpackFile(DESTINATION_DIR + FILE_NAME2 + ".tgz", DESTINATION_DIR);
    Collection files = getXMLFiles(DESTINATION_DIR + FILE_NAME2);
    assertNotNull(files);
    assertEquals(XML_FILES_COUNT, files.size());
  }

  @Test
  void shouldUnpackTheTarGzFilesRecursivelyWithMixedNestedCompressedFiles()
          throws CompressionExtensionNotRecognizedException, IOException {
    gzUnpackingService.unpackFile(DESTINATION_DIR + FILE_NAME3 + ".tar.gz", DESTINATION_DIR);
    Collection files = getXMLFiles(DESTINATION_DIR + FILE_NAME3);
    assertNotNull(files);
    assertEquals(XML_FILES_COUNT, files.size());
  }

  private Collection getXMLFiles(String folderLocation) {
    String[] types = {XML_TYPE};
    return (Collection) FileUtils.listFiles(
            new File(folderLocation),
            types,
            true
    );
  }

  @AfterAll
  static void cleanUp() throws IOException {
    FileUtils.forceDelete(new File(DESTINATION_DIR + FILE_NAME));
    FileUtils.forceDelete(new File(DESTINATION_DIR + FILE_NAME2));
    FileUtils.forceDelete(new File(DESTINATION_DIR + FILE_NAME3));
  }

}