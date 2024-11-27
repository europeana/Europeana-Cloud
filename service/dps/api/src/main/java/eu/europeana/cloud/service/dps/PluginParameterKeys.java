package eu.europeana.cloud.service.dps;

import java.util.HashMap;
import java.util.Map;


/**
 * Parameters for {@link DpsTask}
 */
public final class PluginParameterKeys {

  public static final String XSLT_URL = "XSLT_URL";
  public static final String OUTPUT_URL = "OUTPUT_URL";

  public static final String DPS_TASK_INPUT_DATA = "DPS_TASK_INPUT_DATA";

  public static final String PREVIOUS_TASK_ID = "PREVIOUS_TASK_ID";

  public static final String TASK_NAME = "TASK_NAME";


  // --------- METIS -------------
  public static final String METIS_DATASET_ID = "METIS_DATASET_ID";
  public static final String METIS_DATASET_NAME = "METIS_DATASET_NAME";
  public static final String METIS_DATASET_COUNTRY = "METIS_DATASET_COUNTRY";
  public static final String METIS_DATASET_LANGUAGE = "METIS_DATASET_LANGUAGE";
  public static final String METIS_TARGET_INDEXING_DATABASE = "TARGET_INDEXING_DATABASE";
  public static final String METIS_RECORD_DATE = "RECORD_DATE";
  public static final String METIS_PRESERVE_TIMESTAMPS = "PRESERVE_TIMESTAMPS";
  public static final String DATASET_IDS_TO_REDIRECT_FROM = "DATASET_IDS_TO_REDIRECT_FROM";
  public static final String PERFORM_REDIRECTS = "PERFORM_REDIRECTS";
  public static final String RECORD_IDS_TO_DEPUBLISH = "RECORD_IDS_TO_DEPUBLISH";
  public static final String DEPUBLICATION_REASON = "DEPUBLICATION_REASON";

  // ---------  eCloud  -----------
  public static final String PROVIDER_ID = "PROVIDER_ID";
  public static final String CLOUD_ID = "CLOUD_ID";
  public static final String EUROPEANA_ID = "EUROPEANA_ID";

  public static final String REPRESENTATION_NAME = "REPRESENTATION_NAME";
  public static final String NEW_REPRESENTATION_NAME = "NEW_REPRESENTATION_NAME";
  public static final String REPRESENTATION_VERSION = "REPRESENTATION_VERSION";
  public static final String MIME_TYPE = "MIME_TYPE";
  public static final String OUTPUT_MIME_TYPE = "OUTPUT_MIME_TYPE";
  public static final String OUTPUT_DATA_SETS = "OUTPUT_DATA_SETS";
  public static final String MESSAGE_PROCESSING_START_TIME_IN_MS = "START_TIME";

  public static final String SAMPLE_SIZE = "SAMPLE_SIZE";

  // ---------  IC  -----------
  public static final String OUTPUT_FILE_NAME = "OUTPUT_FILE_NAME";
  //----------  DPS task  ----


  public static final String TOPOLOGY_NAME = "TOPOLOGY_NAME";
  public static final String SENT_DATE = "SENT_DATE";

  //---------- Validation DPS Task ----

  // Input params
  // SCHEMA_NAME - required

  public static final String SCHEMA_NAME = "SCHEMA_NAME";
  public static final String ROOT_LOCATION = "ROOT_LOCATION";
  public static final String SCHEMATRON_LOCATION = "SCHEMATRON_LOCATION";

  public static final String GENERATE_STATS = "GENERATE_STATS";

  //----------Revision ---------------

  public static final String REVISION_NAME = "REVISION_NAME";
  public static final String REVISION_PROVIDER = "REVISION_PROVIDER";
  public static final String REVISION_TIMESTAMP = "REVISION_TIMESTAMP";

  // ---------- OAI-PMH ----------
  public static final String CLOUD_LOCAL_IDENTIFIER = "CLOUD_LOCAL_IDENTIFIER";
  public static final String ADDITIONAL_LOCAL_IDENTIFIER = "ADDITIONAL_LOCAL_IDENTIFIER";
  public static final String INCREMENTAL_HARVEST = "INCREMENTAL_HARVEST";
  public static final String INCREMENTAL_INDEXING = "INCREMENTAL_INDEXING";
  public static final String HARVEST_DATE = "HARVEST_DATE";
  public static final String RECORD_DATESTAMP = "RECORD_DATESTAMP";

  //Media
  public static final String RESOURCE_LINKS_COUNT = "RESOURCE_LINKS_COUNT";
  public static final String RESOURCE_URL = "RESOURCE_URL";
  public static final String EXCEPTION_ERROR_MESSAGE = "EXCEPTION_ERROR_MESSAGE";
  public static final String UNIFIED_ERROR_MESSAGE = "UNIFIED_ERROR_MESSAGE";

  public static final String RESOURCE_LINK_KEY = "RESOURCE_LINK";
  public static final String RESOURCE_METADATA = "RESOURCE_METADATA";

  public static final String MAIN_THUMBNAIL_AVAILABLE = "MAIN_THUMBNAIL_AVAILABLE";

  public static final Map<String, String> PLUGIN_PARAMETERS = new HashMap<>();
  public static final String MARKED_AS_DELETED = "DELETED_RECORD";
  public static final String IGNORED_RECORD = "IGNORED_RECORD";
  public static final String MAXIMUM_PARALLELIZATION = "MAXIMUM_PARALLELIZATION";

  static {
    //the default value for output mimeType
    PLUGIN_PARAMETERS.put(OUTPUT_MIME_TYPE, "text/plain");
    PLUGIN_PARAMETERS.put(REPRESENTATION_NAME, REPRESENTATION_NAME);
    PLUGIN_PARAMETERS.put(NEW_REPRESENTATION_NAME, NEW_REPRESENTATION_NAME);

    PLUGIN_PARAMETERS.put(MIME_TYPE, "text/xml");
  }

  private PluginParameterKeys() {
    throw new UnsupportedOperationException("Pure static class!");
  }

}
