package eu.europeana.cloud.http.service;

import eu.europeana.cloud.http.exceptions.CompressionExtensionNotRecognizedException;

import java.io.IOException;


public interface FileUnpackingService {

  void unpackFile(final String compressedFilePath, final String destinationFolder)
      throws CompressionExtensionNotRecognizedException, IOException;
}
