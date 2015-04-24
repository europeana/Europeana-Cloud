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
        public static final String MIME_TYPE = "MIME_TYPE";
        
        // ---------  Messages  -----------
        public static final String TEXT_STRIPPING_DATASET_MESSAGE = "StripTextFromPdfsInDataset";
        public static final String TEXT_STRIPPING_FILE_MESSAGE = "StripTextFromPdfFile";
        public static final String INDEX_FILE_MESSAGE = "IndexFile";
        
        // ---------  Text stripping  -----------     
        public static final String EXTRACTOR = "EXTRACTOR";
        
        // ---------  Indexer  -----------
        public static final String METADATA = "METADATA";
        public static final String FILE_METADATA = "FILE_METADATA";
        public static final String ORIGINAL_FILE_URL = "ORIGINAL_FILE_URL";
        public static final String ELASTICSEARCH_INDEX = "ELASTICSEARCH_INDEX";
        public static final String ELASTICSEARCH_TYPE = "ELASTICSEARCH_TYPE";
         
}