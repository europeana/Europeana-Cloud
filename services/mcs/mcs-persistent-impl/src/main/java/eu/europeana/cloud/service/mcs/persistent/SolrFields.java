package eu.europeana.cloud.service.mcs.persistent;

/**
 * Constants for solr document field names.
 */
class SolrFields {
    
    private SolrFields() {
        // This class should not have a public or default constructor
    }

    public static final String CLOUD_ID = "cloud_id";

    public static final String VERSION = "version_id";

    public static final String SCHEMA = "schema";

    public static final String PROVIDER_ID = "provider_id";

    public static final String CREATION_DATE = "creation_date";

    public static final String PERSISTENT = "persistent";

    public static final String DATA_SETS = "data_sets";
}
