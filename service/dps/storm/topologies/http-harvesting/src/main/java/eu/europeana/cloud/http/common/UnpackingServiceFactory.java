package eu.europeana.cloud.http.common;

import eu.europeana.cloud.http.exceptions.CompressionExtensionNotRecognizedException;
import eu.europeana.cloud.http.service.FileUnpackingService;
import eu.europeana.cloud.http.service.GzUnpackingService;
import eu.europeana.cloud.http.service.ZipUnpackingService;


public final class UnpackingServiceFactory {

  private UnpackingServiceFactory() {
  }

  private static final ZipUnpackingService ZIP_UNPACKING_SERVICE = new ZipUnpackingService();
  private static final GzUnpackingService GZ_UNPACKING_SERVICE = new GzUnpackingService();

  public static FileUnpackingService createUnpackingService(String compressingExtension)
      throws CompressionExtensionNotRecognizedException {
    if (compressingExtension.equals(CompressionFileExtension.ZIP.getExtension())) {
      return ZIP_UNPACKING_SERVICE;
    } else if (compressingExtension.equals(CompressionFileExtension.GZIP.getExtension())
        || compressingExtension.equals(CompressionFileExtension.TGZIP.getExtension())) {
      return GZ_UNPACKING_SERVICE;
    } else {
      throw new CompressionExtensionNotRecognizedException(
          "This compression extension is not recognized " + compressingExtension);
    }
  }
}
