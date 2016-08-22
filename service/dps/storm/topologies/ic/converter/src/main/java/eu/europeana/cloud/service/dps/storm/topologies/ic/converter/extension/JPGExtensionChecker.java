package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.extension;

import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.common.Extension;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utlis.ExtensionHelper;

/**
 * Utility for checking jpeg extensions of a file full path.
 */
public class JPGExtensionChecker implements ExtensionChecker {
    /**
     * Checking the jpg extension
     *
     * @param filePath the full path of a file
     * @return boolean value checking the jpeg extension  .
     */
    public boolean isGoodExtension(String filePath) {
        return ExtensionHelper.isGoodExtension(filePath, Extension.JPEG.getValues());
    }

}
