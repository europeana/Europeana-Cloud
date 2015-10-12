package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utlis;

/**
 * Created by Tarek on 8/17/2015.
 */

import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.common.Extension;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.common.MimeType;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.exceptions.UnexpectedExtensionsException;

import java.util.HashMap;

/**
 * MimeType-Extension Mapper
 */
public class MimeTypesExtensionsMapper {

    private static HashMap<String, String> mimeTypeMapping;

    static {
        mimeTypeMapping = new HashMap<String, String>() {
            private void put1(String[] keys, String value) {
                for (String key : keys) {
                    if (put(key, value) != null) {
                        throw new IllegalArgumentException("Duplicated extension: "
                                + key);
                    }
                }

            }

            {
                put1(Extension.Tiff.getValues(), MimeType.MIME_IMAGE_TIFF.getValue());
                put1(Extension.Jp2.getValues(), MimeType.MIME_IMAGE_JP2.getValue());

            }
        };
    }

    /**
     * Registers MIME type for provided extension.
     *
     * @param extension the extension
     * @param mimeType  the mimeType
     */
    public static void registerMimeType(String extension, String mimeType) {
        mimeTypeMapping.put(extension, mimeType);
    }


    /**
     * returns MIME type  or throw an exception if no type is found.
     *
     * @param extension the extension
     * @return the mimeType .
     * @throws UnexpectedExtensionsException thrown if extension has null value or wasn't recognised
     */
    public static String lookupMimeType(String extension) throws UnexpectedExtensionsException {
        if (extension == null)
            throw new UnexpectedExtensionsException("This extension can't be null");
        else {
            String mimeType = mimeTypeMapping.get(extension.toLowerCase());
            if (mimeType == null)
                throw new UnexpectedExtensionsException("This extension wasn't recognised");
            return mimeType;
        }
    }
}