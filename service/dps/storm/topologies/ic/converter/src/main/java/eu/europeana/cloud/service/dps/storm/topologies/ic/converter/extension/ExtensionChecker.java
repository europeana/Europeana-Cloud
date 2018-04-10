package eu.europeana.cloud.service.dps.storm.topologies.ic.converter.extension;

/**
 * Utility for checking the extension of the file full path.
 */
public interface ExtensionChecker {
    /**
     * Checking the file extension based on the context
     *
     * @param filePath the full path of a file
     * @return boolean value based on the checking process  .
     */
    boolean isGoodExtension(String filePath);

}
