package eu.europeana.cloud.common.web;

/**
 * PathConstants
 */
public interface ParamConstants {

    // resources' paths
    String RECORDS = "records";

    String REPRESENTATIONS = "representations";

    String VERSIONS = "versions";

    @Deprecated
    String P_CLOUDID = "CLOUDID";

    String CLOUD_ID = "cloudId";

    String REPRESENTATION_NAME = "representationName";

    String VERSION = "version";

    @Deprecated
    String P_PROVIDER = "DATAPROVIDER";

    String PROVIDER_ID = "providerId";

    String DATA_SET_ID = "dataSetId";

    String REVISION_NAME= "revisionName";

    String REVISION_PROVIDER = "revisionProvider";

    String REVISION_PROVIDER_ID = "revisionProviderId";

    String FILE_NAME = "fileName";

    @Deprecated
    String P_LOCALID = "LOCALID";

    String LOCAL_ID = "localId";

    String USER_NAME = "userName";

    String PERMISSION = "permission";

    String TAG = "tag";

    // form parameters' names (also used as query parameters' names)
    String F_DATASET = "dataSetId";

    String F_DESCRIPTION = "description";

    String F_PROVIDER = "providerId";

    String F_CLOUDID = "cloudId";

    String F_REPRESENTATIONNAME = "representationName";

    String F_VER = "version";

    String F_FILE_DATA = "data";

    String F_FILE_MIME = "mimeType";

    String F_FILE_NAME = "fileName";

    String F_DATE_FROM = "creationDateFrom";

    String F_START_FROM = "startFrom";

    String F_LIMIT = "limit";

    String F_TAG = "tag";

    String F_TAGS = "tags";

    String IS_DELETED = "deleted";

    String F_REVISION_TIMESTAMP = "revisionTimestamp";

    String F_EXISTING_ONLY = "existingOnly";

    String F_REVISION_PROVIDER_ID = "revisionProviderId";

    //header paramiters' names
    String H_RANGE = "Range";

    String F_PERMISSION = "permission";

    String F_USERNAME = "username";

}
