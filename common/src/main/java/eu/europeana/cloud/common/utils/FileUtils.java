package eu.europeana.cloud.common.utils;

/**
 * Created by helin on 2015-10-15.
 */
public class FileUtils {

    /**
     * Generates unique key for file content.
     *
     * @param cloudId
     * @param representationName
     * @param version
     * @param fileName
     * @return
     */
    public static String generateKeyForFile(String cloudId, String representationName, String version, String fileName) {
        if (cloudId == null || cloudId.isEmpty()
                || representationName == null || representationName.isEmpty()
                || version == null || version.isEmpty()
                || fileName == null || fileName.isEmpty()) {
            return null;
        }
        return cloudId + "_" + representationName + "_" + version + "_" + fileName;
    }
}
