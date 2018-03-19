package eu.europeana.cloud.http.common;

import eu.europeana.cloud.http.exceptions.CompressionExtensionNotRecognizedException;
import eu.europeana.cloud.http.service.FileUnpackingService;
import eu.europeana.cloud.http.service.GzUnpackingService;
import eu.europeana.cloud.http.service.ZipUnpackingService;


public class UnpackingServiceFactory {
    private static final ZipUnpackingService zipUnpackingService = new ZipUnpackingService();
    private static final GzUnpackingService gzUnpackingService = new GzUnpackingService();

    public static FileUnpackingService createUnpackingService(String compressingExtension) throws CompressionExtensionNotRecognizedException {
        if (compressingExtension.equals(CompressionFileExtension.ZIP.getExtension()))
            return zipUnpackingService;
        else if (compressingExtension.equals(CompressionFileExtension.GZIP.getExtension()) || compressingExtension.equals(CompressionFileExtension.TGZIP.getExtension()))
            return gzUnpackingService;
        else
            throw new CompressionExtensionNotRecognizedException("This compression extension is not recognized " + compressingExtension);
    }
}
