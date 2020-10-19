package eu.europeana.cloud.service.dps.http;

import java.io.IOException;


public interface FileUnpackingService {
    void unpackFile(final String compressedFilePath, final String destinationFolder) throws CompressionExtensionNotRecognizedException, IOException;
}
