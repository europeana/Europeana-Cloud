package eu.europeana.cloud.common.web;

/**
 * PathConstants
 */
public interface ParamConstants {

    String LATEST_VERSION_KEYWORD = "LATEST";

    // resources' paths
    String RECORDS = "records";

    String REPRESENTATIONS = "representations";

    String VERSIONS = "versions";

    String PROVIDERS = "data-providers";

    String DATASETS = "data-sets";

    String ASSIGNMENTS = "assignments";

    String REVISIONS = "revisions";

    String COPY = "copy";

    String PERSIST = "persist";

    String PERMIT = "permit";

    // path parameters' names
    @Deprecated
    String P_CLOUDID = "CLOUDID";

    String CLOUD_ID = "cloudId";

    @Deprecated
    String P_REPRESENTATIONNAME = "REPRESENTATIONNAME";


    String REPRESENTATION_NAME = "representationName";


    //String P_REVISIONID = "P_REVISIONID";

    @Deprecated
    String P_VER = "VERSION";

    String VERSION = "version";

    @Deprecated
    String P_PROVIDER = "DATAPROVIDER";

    String PROVIDER_ID = "providerId";

    @Deprecated
    String P_DATASET = "DATASET";

    String DATA_SET_ID = "dataSetId";

    String REVISION_NAME= "revisionName";

    String REVISION_PROVIDER = "revisionProvider";

    String REVISION_PROVIDER_ID = "revisionProviderId";

    @Deprecated
    String P_FILENAME = "FILENAME";

    String FILE_NAME = "fileName";

    @Deprecated
    String P_LOCALID = "LOCALID";

    String LOCAL_ID = "localId";

    @Deprecated
    String P_USERNAME = "USERNAME";

    String USER_NAME = "userName";

    @Deprecated
    String P_PERMISSION_TYPE = "PERMISSION_TYPE";

    String PERMISSION = "permission";

    @Deprecated
    String P_TAG = "TAG";

    String TAG = "tag";


    //String P_TAGS = "TAGS";

    // form parameters' names (also used as query parameters' names)
    String F_DATASET = "dataSetId";

    String F_DATASET_PROVIDER_ID = "dataSetProviderId";

    String F_DESCRIPTION = "description";

    String F_PROVIDER = "providerId";

    String F_CLOUDID = "cloudId";

    String F_REPRESENTATIONNAME = "representationName";

    String F_VER = "version";

    String F_FILE_DATA = "data";

    String F_FILE_MIME = "mimeType";

    String F_FILE_NAME = "fileName";

    String F_DATE_FROM = "creationDateFrom";

    String F_PERSISTENT = "persistent";

    String F_DATE_UNTIL = "creationDateUntil";

    String F_START_FROM = "startFrom";

    String F_LIMIT = "limit";

    String F_REPRESENTATION = "representation";

    String F_DATASETS = "dataSets";

    String F_TAG = "tag";

    String F_TAGS = "tags";

    String IS_DELETED = "deleted";

    String F_REVISION_TIMESTAMP = "revisionTimestamp";

    String F_REVISION_PROVIDER_ID = "revisionProviderId";

    String P_REVISION_NAME = "REVISION_NAME";

    String P_REVISION_PROVIDER_ID = "REVISION_PROVIDER_ID";

    String F_REVISION_NAME = "revisionName";

    //header paramiters' names
    String H_RANGE = "Range";

}
