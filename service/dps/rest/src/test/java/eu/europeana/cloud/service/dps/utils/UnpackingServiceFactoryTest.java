package eu.europeana.cloud.service.dps.utils;

import eu.europeana.cloud.service.dps.http.*;
import org.junit.Test;

import static org.junit.Assert.assertTrue;


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