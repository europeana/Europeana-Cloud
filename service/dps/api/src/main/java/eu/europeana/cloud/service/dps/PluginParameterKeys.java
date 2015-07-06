package eu.europeana.cloud.service.dps;

/**
 * Parameters for {@link DpsTask}
 */
public final class PluginParameterKeys {
	
	private PluginParameterKeys() {
	}

	public static final String XSLT_URL = "XSLT_URL";	
	public static final String OUTPUT_URL = "OUTPUT_URL";
        
        public static final String FILE_URL = "FILE_URL";
        public static final String FILE_DATA = "FILE_DATA";
        
        // ---------  eCloud  -----------
        public static final String PROVIDER_ID = "PROVIDER_ID";
        public static final String DATASET_ID = "DATASET_ID";
        public static final String CLOUD_ID = "CLOUD_ID";     
        public static final String REPRESENTATION_NAME = "REPRESENTATION_NAME";
        public static final String REPRESENTATION_VERSION = "REPRESENTATION_VERSION";
        public static final String FILE_NAME = "FILE_NAME";
        public static final String MIME_TYPE = "MIME_TYPE";
        
        // ---------  Messages  -----------
        public static final String NEW_DATASET_MESSAGE = "NewDataset";
        public static final String NEW_FILE_MESSAGE = "NewFile";       
        public static final String NEW_EXTRACTED_DATA_MESSAGE = "NewExtractedData";
        public static final String NEW_ASSOCIATION_MESSAGE = "NewAssociation";
        public static final String NEW_INDEX_MESSAGE = "NewIndex";
        
        public static final String INDEX_FILE_MESSAGE = "IndexFile";
        
        
        // ---------  Text stripping  ----------- 
        public static final String EXTRACT_TEXT = "EXTRACT_TEXT";
        public static final String EXTRACTOR = "EXTRACTOR";
        public static final String STORE_EXTRACTED_TEXT = "STORE_EXTRACTED_TEXT";
        
        // ---------  Indexer  -----------
        public static final String INDEXER = "INDEXER";
        public static final String INDEX_DATA = "INDEX_DATA";
        public static final String METADATA = "METADATA";
        public static final String FILE_METADATA = "FILE_METADATA";
        public static final String ORIGINAL_FILE_URL = "ORIGINAL_FILE_URL";               
}