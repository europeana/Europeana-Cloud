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

    // path parameters' names
    static final String P_GID = "ID";

    static final String P_SCHEMA = "SCHEMA";

    static final String P_VER = "VERSION";

    static final String P_PROVIDER = "DATAPROVIDER";

    static final String P_DATASET = "DATASET";

    static final String P_FILE = "FILE";
    
    static final String P_LOCALID = "LOCALID";
    
    static final String P_CLOUDID = "CLOUDID";

    // form parameters' names (also used as query parameters' names)
    static final String F_DATASET = "dataSetId";

    static final String F_DATASET_PROVIDER_ID = "dataSetProviderId";

    static final String F_DESCRIPTION = "description";

    static final String F_PROVIDER = "providerId";

    static final String F_GID = "recordId";

    static final String F_SCHEMA = "schema";

    static final String F_VER = "version";

    static final String F_FILE_DATA = "data";

    static final String F_FILE_MIME = "mimeType";

    static final String F_DATE_FROM = "creationDateFrom";

    static final String F_PERSISTENT = "persistent";

    static final String F_DATE_UNTIL = "creationDateUntil";

    static final String F_START_FROM = "startFrom";
    

}
