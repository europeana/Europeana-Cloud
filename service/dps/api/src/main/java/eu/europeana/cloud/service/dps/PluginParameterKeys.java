package eu.europeana.cloud.service.dps;

import java.util.HashMap;
import java.util.Map;


/**
 * Parameters for {@link DpsTask}
 */
public final class PluginParameterKeys {
    public static final String XSLT_URL = "XSLT_URL";
    public static final String OUTPUT_URL = "OUTPUT_URL";

    public static final String DATASET_URL = "DATASET_URL";

    public static final String DPS_TASK_INPUT_DATA = "DPS_TASK_INPUT_DATA";

    public static final String PREVIOUS_TASK_ID = "PREVIOUS_TASK_ID";

    public static final String TASK_NAME = "TASK_NAME";


    // --------- METIS -------------
    public static final String METIS_DATASET_ID = "METIS_DATASET_ID";
    public static final String METIS_DATASET_NAME = "METIS_DATASET_NAME";
    public static final String METIS_DATASET_COUNTRY = "METIS_DATASET_COUNTRY";
    public static final String METIS_DATASET_LANGUAGE = "METIS_DATASET_LANGUAGE";
    public static final String METIS_TARGET_INDEXING_DATABASE = "TARGET_INDEXING_DATABASE";
    public static final String METIS_USE_ALT_INDEXING_ENV = "USE_ALT_INDEXING_ENV";
    public static final String METIS_PRESERVE_TIMESTAMPS = "PRESERVE_TIMESTAMPS";


    // ---------  eCloud  -----------
    public static final String PROVIDER_ID = "PROVIDER_ID";
    public static final String CLOUD_ID = "CLOUD_ID";

    public static final String REPRESENTATION_NAME = "REPRESENTATION_NAME";
    public static final String NEW_REPRESENTATION_NAME = "NEW_REPRESENTATION_NAME";
    public static final String REPRESENTATION_VERSION = "REPRESENTATION_VERSION";
    public static final String MIME_TYPE = "MIME_TYPE";
    public static final String OUTPUT_MIME_TYPE = "OUTPUT_MIME_TYPE";
    public static final String AUTHORIZATION_HEADER = "AUTHORIZATION_HEADER";
    public static final String OUTPUT_DATA_SETS = "OUTPUT_DATA_SETS";


    // ---------  IC  -----------
    public static final String OUTPUT_FILE_NAME = "OUTPUT_FILE_NAME";
    public static final String KAKADU_ARGUEMENTS = "KAKADU_ARGUEMENTS";
    //----------  DPS task  ----


    public static final String REPRESENTATION = "REPRESENTATION";
    public static final String TOPOLOGY_NAME = "TOPOLOGY_NAME";

    //---------- Validation DPS Task ----

    // Input params
    // SCHEMA_NAME - required

    public static final String SCHEMA_NAME = "SCHEMA_NAME";
    public static final String ROOT_LOCATION = "ROOT_LOCATION";
    public static final String SCHEMATRON_LOCATION = "SCHEMATRON_LOCATION";

    //----------  OAI-PMH harvesting DPS task  ----


    //----------Revision ---------------

    public static final String REVISION_NAME = "REVISION_NAME";
    public static final String REVISION_PROVIDER = "REVISION_PROVIDER";
    public static final String REVISION_TIMESTAMP = "REVISION_TIMESTAMP";

    // ---------- OAI-PMH ----------
    public static final String CLOUD_LOCAL_IDENTIFIER = "CLOUD_LOCAL_IDENTIFIER";
    public static final String ADDITIONAL_LOCAL_IDENTIFIER = "ADDITIONAL_LOCAL_IDENTIFIER";
    public static final String USE_DEFAULT_IDENTIFIERS = "USE_DEFAULT_IDENTIFIERS";

    // -----------MIGRATION----------
    public static final String MIGRATION_IDENTIFIER_PREFIX = "MIGRATION_IDENTIFIER_PREFIX";

    //Media
    public static final String RESOURCE_LINKS_COUNT = "RESOURCE_LINKS_COUNT";
    public static final String EXCEPTION_ERROR_MESSAGE = "EXCEPTION_ERROR_MESSAGE";
    public static final String RESOURCE_LINK_KEY = "RESOURCE_LINK";
    public static final String RESOURCE_METADATA = "RESOURCE_METADATA";


    public static final Map<String, String> PLUGIN_PARAMETERS = new HashMap<>();

    static {

        //the default value for output mimeType
        PLUGIN_PARAMETERS.put(OUTPUT_MIME_TYPE, "text/plain");
        PLUGIN_PARAMETERS.put(REPRESENTATION_NAME, "REPRESENTATION_NAME");
        PLUGIN_PARAMETERS.put(NEW_REPRESENTATION_NAME, "NEW_REPRESENTATION_NAME");

        PLUGIN_PARAMETERS.put(MIME_TYPE, "text/xml");
        PLUGIN_PARAMETERS.put(KAKADU_ARGUEMENTS, "-rate 1.0,0.84,0.7,0.6,0.5,0.4,0.35,0.3,0.25,0.21,0.18,0.15,0.125,0.1,0.088,0.075,0.0625,0.05,0.04419,0.03716,0.03125,0.025,0.0221,0.01858,0.015625 Clevels=6 Cmodes={BYPASS} Corder=RLCP -no_palette");


    }

    private PluginParameterKeys() {
        throw new UnsupportedOperationException("Pure static class!");
    }

}
