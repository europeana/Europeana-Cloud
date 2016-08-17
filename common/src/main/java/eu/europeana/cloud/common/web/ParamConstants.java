package eu.europeana.cloud.common.web;

/**
 * PathConstants
 */
public interface ParamConstants {

    static final String LATEST_VERSION_KEYWORD = "LATEST";

    // resources' paths
    static final String RECORDS = "records";

    static final String REPRESENTATIONS = "representations";

    static final String VERSIONS = "versions";

    static final String PROVIDERS = "data-providers";

    static final String DATASETS = "data-sets";

    static final String ASSIGNMENTS = "assignments";

    static final String COPY = "copy";

    static final String PERSIST = "persist";

    static final String PERMIT = "permit";

    // path parameters' names
    static final String P_CLOUDID = "CLOUDID";

    static final String P_REPRESENTATIONNAME = "REPRESENTATIONNAME";

    static final String P_REVISIONID = "P_REVISIONID";

    static final String P_VER = "VERSION";

    static final String P_PROVIDER = "DATAPROVIDER";

    static final String P_DATASET = "DATASET";

    static final String P_FILENAME = "FILENAME";

    static final String P_LOCALID = "LOCALID";
    
    static final String P_USERNAME = "USERNAME";
    
    static final String P_PERMISSION_TYPE = "PERMISSION_TYPE";

    // form parameters' names (also used as query parameters' names)
    static final String F_DATASET = "dataSetId";

    static final String F_DATASET_PROVIDER_ID = "dataSetProviderId";

    static final String F_DESCRIPTION = "description";

    static final String F_PROVIDER = "providerId";

    static final String F_CLOUDID = "cloudId";

    static final String F_REPRESENTATIONNAME = "representationName";

    static final String F_VER = "version";

    static final String F_FILE_DATA = "data";

    static final String F_FILE_MIME = "mimeType";

    static final String F_FILE_NAME = "fileName";

    static final String F_DATE_FROM = "creationDateFrom";

    static final String F_PERSISTENT = "persistent";

    static final String F_DATE_UNTIL = "creationDateUntil";

    static final String F_START_FROM = "startFrom";

    static final String F_LIMIT = "limit";

    static final String F_REPRESENTATION = "representation";

    static final String F_DATASETS = "dataSets";

    //header paramiters' names
    static final String H_RANGE = "Range";

}
