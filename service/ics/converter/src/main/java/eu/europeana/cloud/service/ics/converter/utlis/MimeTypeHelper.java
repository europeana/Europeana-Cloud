package eu.europeana.cloud.service.ics.converter.utlis;

import eu.europeana.cloud.service.ics.converter.exceptions.UnexpectedExtensionsException;
import org.apache.tika.Tika;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by Tarek on 8/28/2015.
 */

/**
 * Utility for Mimetype-related services
 */
public class MimeTypeHelper {

    /**
     * Gets the mimeType based on an extension
     *
     * @param extension the mimetype
     * @return the mimeType .
     * @throws UnexpectedExtensionsException
     *             thrown if extension wasn't recognized
     */
    public static String getMimeTypeFromExtension(String extension) throws UnexpectedExtensionsException{
        return MimeTypesExtensionsMapper.lookupMimeType(extension);
    }

    /**
     Gets the mimeType based based on inputStream
     *
     * @param inputStream the stream
     * @return the mimeType .
     * @throws IOException
     *             thrown if the stream throws the exception
     */
    public static String getMimeTypeFromStream(InputStream inputStream) throws IOException {
        Tika tika = new Tika();
        String mimeType = tika.detect(inputStream);
        return mimeType;
    }

}
