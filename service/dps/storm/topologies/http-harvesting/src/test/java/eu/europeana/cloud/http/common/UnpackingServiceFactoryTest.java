package eu.europeana.cloud.http.common;

import static org.junit.Assert.assertTrue;

import eu.europeana.cloud.http.exceptions.CompressionExtensionNotRecognizedException;
import eu.europeana.cloud.http.service.FileUnpackingService;
import eu.europeana.cloud.http.service.GzUnpackingService;
import eu.europeana.cloud.http.service.ZipUnpackingService;
import org.junit.Test;


public class UnpackingServiceFactoryTest {

  private final static String ZIP_EXTENSION = "zip";
  private final static String GZIP_EXTENSION = "gz";
  private final static String TGZIP_EXTENSION = "tgz";
  private final static String UNDEFINED_COMPRESSION_EXTENSION = "UNDEFINED_EXTENSION";

  @Test
  public void shouldReturnZipService() throws CompressionExtensionNotRecognizedException {
    FileUnpackingService fileUnpackingService = UnpackingServiceFactory.createUnpackingService(ZIP_EXTENSION);
    assertTrue(fileUnpackingService instanceof ZipUnpackingService);

  }

  @Test
  public void shouldReturnGZipService() throws CompressionExtensionNotRecognizedException {
    FileUnpackingService fileUnpackingService = UnpackingServiceFactory.createUnpackingService(GZIP_EXTENSION);
    assertTrue(fileUnpackingService instanceof GzUnpackingService);

  }

  @Test
  public void shouldReturnGZipServiceFotTGZExtension() throws CompressionExtensionNotRecognizedException {
    FileUnpackingService fileUnpackingService = UnpackingServiceFactory.createUnpackingService(TGZIP_EXTENSION);
    assertTrue(fileUnpackingService instanceof GzUnpackingService);
  }

  @Test(expected = CompressionExtensionNotRecognizedException.class)
  public void shouldThrowExceptionIfTheExTensionWasNotRecognized() throws CompressionExtensionNotRecognizedException {
    UnpackingServiceFactory.createUnpackingService(UNDEFINED_COMPRESSION_EXTENSION);
  }

}