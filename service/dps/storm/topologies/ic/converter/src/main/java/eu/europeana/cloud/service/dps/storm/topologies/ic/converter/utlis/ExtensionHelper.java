package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utlis;


import org.apache.tika.config.TikaConfig;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypeException;

/**
 * Utility for extension-related services
 */
public class ExtensionHelper {
    /**
     * Checks the file extension based on array of values
     *
     * @param filePath   the full path of a file
     * @param extensions an array of accepted extensions
     * @return boolean value based on the checking process  .
     */
    public static boolean isGoodExtension(String filePath, String[] extensions) {
        if (filePath != null) {
            for (final String ext : extensions)
                if (filePath.toLowerCase().endsWith("." + ext))
                    return true;
        }
        return false;
    }

    /**
     * Gets the extension based on a mime type
     *
     * @param type the mimetype
     * @return the extension .
     * @throws MimeTypeException thrown if MimeType wasn't detected
     */
    public static String getExtension(String type) throws MimeTypeException {

        TikaConfig config = TikaConfig.getDefaultConfig();
        MimeType mimeType = config.getMimeRepository().forName(type);
        String extension = mimeType.getExtension();
        return extension;

    }

}
