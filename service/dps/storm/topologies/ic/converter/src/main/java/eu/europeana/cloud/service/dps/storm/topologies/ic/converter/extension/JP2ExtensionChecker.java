package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.extension;


import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.common.Extension;
import eu.europeana.cloud.service.dps.storm.topologies.ic.converter.utlis.ExtensionHelper;

/**
 * Utility for checking JP2 extensions of a file full path.
 */
public class JP2ExtensionChecker implements ExtensionChecker {
    /**
     * Checking the jp2file extension
     *
     * @param filePath the full path of a file
     * @return boolean value checking the jp2 extension  .
     */
    public boolean isGoodExtension(String filePath) {
        return ExtensionHelper.isGoodExtension(filePath, Extension.Jp2.getValues());
    }

}
