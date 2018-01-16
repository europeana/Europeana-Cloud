package eu.europeana.cloud.service.dps;

import java.util.HashMap;
import java.util.Map;

import static eu.europeana.cloud.service.dps.InputDataType.DATASET_URLS;
import static eu.europeana.cloud.service.dps.InputDataType.FILE_URLS;

/**
 * Parameters for {@link DpsTask}
 */
public final class PluginParameterKeys {
    public static final String XSLT_URL = "XSLT_URL";
    public static final String OUTPUT_URL = "OUTPUT_URL";

    public static final String FILE_URL = "FILE_URL";
    public static final String DATASET_URL = "DATASET_URL";
    public static final String FILE_DATA = "FILE_DATA";

    public static final String DPS_TASK_INPUT_DATA = "DPS_TASK_INPUT_DATA";

    public static final String TASK_NAME = "TASK_NAME";
    public static final String ROUTING = "ROUTING";

    // ---------  eCloud  -----------
    public static final String PROVIDER_ID = "PROVIDER_ID";
    public static final String DATASET_ID = "DATASET_ID";
    public static final String CLOUD_ID = "CLOUD_ID";
    public static final String LOCAL_ID = "LOCAL_ID";
    public static final String REPRESENTATION_NAME = "REPRESENTATION_NAME";
    public static final String NEW_REPRESENTATION_NAME = "NEW_REPRESENTATION_NAME";
    public static final String REPRESENTATION_VERSION = "REPRESENTATION_VERSION";
    public static final String FILE_NAME = "FILE_NAME";
    public static final String MIME_TYPE = "MIME_TYPE";
    public static final String OUTPUT_MIME_TYPE = "OUTPUT_MIME_TYPE";
    public static final String AUTHORIZATION_HEADER = "AUTHORIZATION_HEADER";
    public static final String OUTPUT_DATA_SETS = "OUTPUT_DATA_SETS";


    // ---------  Messages  -----------
    public static final String NEW_DATASET_MESSAGE = "NewDataset";
    public static final String NEW_FILE_MESSAGE = "NewFile";
    public static final String NEW_EXTRACTED_DATA_MESSAGE = "NewExtractedData";
    public static final String NEW_ANNOTATION_MESSAGE = "NewAnnotation";
    public static final String NEW_INDEX_MESSAGE = "NewIndex";

    public static final String INDEX_FILE_MESSAGE = "IndexFile";

    // ---------  Text stripping  -----------
    public static final String EXTRACT_TEXT = "EXTRACT_TEXT";                   //true or false
    public static final String EXTRACTORS = "EXTRACTORS";                       //Map<"type": "extractor_name">
    public static final String FILE_FORMATS = "FILE_FORMATS";                    //Map<"representatnionName","type">
    public static final String STORE_EXTRACTED_TEXT = "STORE_EXTRACTED_TEXT";   //true or false

    // ---------  Indexer  -----------
    public static final String INDEXER = "INDEXER";                         //name of indexer
    public static final String INDEX_DATA = "INDEX_DATA";                   //true or false
    public static final String FILE_METADATA = "FILE_METADATA";             //e.g. metadata from PDF file
    public static final String ORIGINAL_FILE_URL = "ORIGINAL_FILE_URL";     //e.g. url to PDF file thich contains extracted text

    // ---------  IC  -----------
    public static final String OUTPUT_FILE_NAME = "OUTPUT_FILE_NAME";
    public static final String KAKADU_ARGUEMENTS = "KAKADU_ARGUEMENTS";
    //----------  DPS task  ----

    // InputDataType.DATASET_URLS
    // InputDataType.FILE_URLS
    public static final String REPRESENTATION = "REPRESENTATION";
    public static final String TOPOLOGY_NAME = "TOPOLOGY_NAME";

    //---------- Validation DPS Task ----

    // Input params
    // SCHEMA_NAME - required

    public static final String SCHEMA_NAME = "SCHEMA_NAME";
    public static final String ROOT_LOCATION = "ROOT_LOCATION";

    //----------  OAI-PMH harvesting DPS task  ----

    // Input params
    // DPS_TASK_INPUT_DATA - required

    // HARVESTING_DETAILS - OAIPMHHarvestingDetails.java - optional

    //----------Revision ---------------

    public static final String REVISION_NAME = "REVISION_NAME";
    public static final String REVISION_PROVIDER = "REVISION_PROVIDER";
    public static final String REVISION_TIMESTAMP = "REVISION_TIMESTAMP";
    public static final String INTERVAL = "INTERVAL";

    // ---------- OAI-PMH ----------
    public static final String OAI_IDENTIFIER = "OAI_IDENTIFIER";

    public static Map<String, String> PLUGIN_PARAMETERS = new HashMap<>();

    static {
        PLUGIN_PARAMETERS.put(XSLT_URL, "XSLT_URL");
        PLUGIN_PARAMETERS.put(OUTPUT_URL, "OUTPUT_URL");

        PLUGIN_PARAMETERS.put(FILE_URL, "FILE_URL");
        PLUGIN_PARAMETERS.put(DATASET_URL, "DATASET_URL");
        PLUGIN_PARAMETERS.put(FILE_DATA, "FILE_DATA");

        PLUGIN_PARAMETERS.put(DPS_TASK_INPUT_DATA, "DPS_TASK_INPUT_DATA");

        //the default value for output mimeType
        PLUGIN_PARAMETERS.put(OUTPUT_MIME_TYPE, "text/plain");

        PLUGIN_PARAMETERS.put(TASK_NAME, "TASK_NAME");
        PLUGIN_PARAMETERS.put(ROUTING, "ROUTING");
        PLUGIN_PARAMETERS.put(PROVIDER_ID, "PROVIDER_ID");
        PLUGIN_PARAMETERS.put(DATASET_ID, "DATASET_ID");
        PLUGIN_PARAMETERS.put(CLOUD_ID, "CLOUD_ID");
        PLUGIN_PARAMETERS.put(REPRESENTATION_NAME, "REPRESENTATION_NAME");
        PLUGIN_PARAMETERS.put(NEW_REPRESENTATION_NAME, "NEW_REPRESENTATION_NAME");
        PLUGIN_PARAMETERS.put(REPRESENTATION_VERSION, "REPRESENTATION_VERSION");
        PLUGIN_PARAMETERS.put(FILE_NAME, "FILE_NAME");
        PLUGIN_PARAMETERS.put(MIME_TYPE, "text/xml");

        PLUGIN_PARAMETERS.put(NEW_DATASET_MESSAGE, "NewDataset");
        PLUGIN_PARAMETERS.put(NEW_FILE_MESSAGE, "NewFile");
        PLUGIN_PARAMETERS.put(NEW_EXTRACTED_DATA_MESSAGE, "NewExtractedData");
        PLUGIN_PARAMETERS.put(NEW_ANNOTATION_MESSAGE, "NewAnnotation");
        PLUGIN_PARAMETERS.put(NEW_INDEX_MESSAGE, "NewIndex");

        PLUGIN_PARAMETERS.put(INDEX_FILE_MESSAGE, "IndexFile");

        // ---------  Text stripping  -----------
        PLUGIN_PARAMETERS.put(EXTRACT_TEXT, "EXTRACT_TEXT");                   //true or false
        PLUGIN_PARAMETERS.put(EXTRACTORS, "EXTRACTORS");                       //Map<"type": "extractor_name">
        PLUGIN_PARAMETERS.put(FILE_FORMATS, "FILE_FORMATS");                    //Map<"representatnionName","type">
        PLUGIN_PARAMETERS.put(STORE_EXTRACTED_TEXT, "STORE_EXTRACTED_TEXT");   //true or false

        // ---------  Indexer  -----------
        PLUGIN_PARAMETERS.put(INDEXER, "INDEXER");                         //name of indexer
        PLUGIN_PARAMETERS.put(INDEX_DATA, "INDEX_DATA");                   //true or false
        PLUGIN_PARAMETERS.put(FILE_METADATA, "FILE_METADATA");             //e.g. metadata from PDF file
        PLUGIN_PARAMETERS.put(ORIGINAL_FILE_URL, "ORIGINAL_FILE_URL"); //e.g. url to PDF file thich contains extracted text

        // ---------  IC  -----------

        PLUGIN_PARAMETERS.put(KAKADU_ARGUEMENTS, "-rate 1.0,0.84,0.7,0.6,0.5,0.4,0.35,0.3,0.25,0.21,0.18,0.15,0.125,0.1,0.088,0.075,0.0625,0.05,0.04419,0.03716,0.03125,0.025,0.0221,0.01858,0.015625 Clevels=6 Cmodes={BYPASS} Corder=RLCP -no_palette");

        //----------- DPS task --------
        PLUGIN_PARAMETERS.put(DATASET_URLS.name(), "DATASET_URLS");

        /* File URL Key */
        PLUGIN_PARAMETERS.put(FILE_URLS.name(), "FILE_URLS");
    }

    private PluginParameterKeys() {
        throw new UnsupportedOperationException("Pure static class!");
    }

}
