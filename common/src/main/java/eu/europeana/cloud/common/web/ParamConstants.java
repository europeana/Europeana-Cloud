package eu.europeana.cloud.common.web;

/**
 * PathConstants
 */
public final class ParamConstants {


  // resources' paths
  public static final String RECORDS = "records";

  public static final String REPRESENTATIONS = "representations";

  public static final String VERSIONS = "versions";

  @Deprecated
  public static final String P_CLOUDID = "CLOUDID";

  public static final String CLOUD_ID = "cloudId";

  public static final String REPRESENTATION_NAME = "representationName";

  public static final String VERSION = "version";

  @Deprecated
  public static final String P_PROVIDER = "DATAPROVIDER";

  public static final String PROVIDER_ID = "providerId";

  public static final String DATA_SET_ID = "dataSetId";

  public static final String REVISION_NAME = "revisionName";

  public static final String REVISION_PROVIDER = "revisionProvider";

  public static final String REVISION_PROVIDER_ID = "revisionProviderId";

  public static final String FILE_NAME = "fileName";
  public static final String MARK_DELETED = "markDeleted";

  @Deprecated
  public static final String P_LOCALID = "LOCALID";

  public static final String LOCAL_ID = "localId";

  public static final String USER_NAME = "userName";

  public static final String PERMISSION = "permission";

  public static final String TAG = "tag";

  // form parameters' names (also used as query parameters' names)
  public static final String F_DATASET = "dataSetId";

  public static final String F_DESCRIPTION = "description";

  public static final String F_PROVIDER = "providerId";

  public static final String F_CLOUDID = "cloudId";

  public static final String F_REPRESENTATIONNAME = "representationName";

  public static final String F_VER = "version";

  public static final String F_FILE_DATA = "data";

  public static final String F_FILE_MIME = "mimeType";

  public static final String F_FILE_NAME = "fileName";

  public static final String F_DATE_FROM = "creationDateFrom";

  public static final String F_START_FROM = "startFrom";

  public static final String F_LIMIT = "limit";

  public static final String F_TAG = "tag";

  public static final String F_TAGS = "tags";

  public static final String IS_DELETED = "deleted";

  public static final String F_REVISION_TIMESTAMP = "revisionTimestamp";

  public static final String F_EXISTING_ONLY = "existingOnly";

  public static final String F_REVISION_PROVIDER_ID = "revisionProviderId";

  //header paramiters' names
  public static final String H_RANGE = "Range";

  public static final String F_PERMISSION = "permission";

  public static final String F_USERNAME = "username";

  private ParamConstants() {
  }
}
