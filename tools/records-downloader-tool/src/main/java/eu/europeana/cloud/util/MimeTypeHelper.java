package eu.europeana.cloud.util;


import org.apache.tika.config.TikaConfig;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypeException;

import java.io.IOException;
import java.io.InputStream;

/**
 * Utility for extension-related services
 */
public class MimeTypeHelper {

  private static final TikaConfig CONFIG = TikaConfig.getDefaultConfig();

  /**
   * Gets the extension based on a media type
   *
   * @param mediaType MediaType
   * @return the extension .
   * @throws MimeTypeException thrown if MimeType wasn't detected
   */
  public static String getExtension(MediaType mediaType) throws MimeTypeException {
    MimeType mimeType = CONFIG.getMimeRepository().forName(mediaType.toString());
    return mimeType.getExtension();

  }

  /**
   * Gets Media Type based on a stream
   *
   * @param stream
   * @return the mediaType .
   * @throws MimeTypeException thrown if MimeType wasn't detected
   * @throws IOException
   */
  public static MediaType getMediaTypeFromStream(InputStream stream) throws MimeTypeException, IOException {
    return CONFIG.getMimeRepository().detect(stream, new Metadata());

  }

}
