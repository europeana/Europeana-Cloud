package eu.europeana.cloud.common.utils;

/**
 * Created by helin on 2015-10-15.
 */
public final class FileUtils {

  private FileUtils() {
  }

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
    boolean ifCloudIdIsAbsent = cloudId == null || cloudId.isEmpty();
    boolean ifRepresentationIsAbsent= representationName == null || representationName.isEmpty();
    boolean ifVersionIsAbsent = version == null || version.isEmpty();
    boolean ifFileNameIsAbsent = fileName == null || fileName.isEmpty();
    if ( ifCloudIdIsAbsent
        || ifRepresentationIsAbsent
        || ifVersionIsAbsent
        || ifFileNameIsAbsent) {
      return null;
    }
    return cloudId + "_" + representationName + "_" + version + "_" + fileName;
  }
}
