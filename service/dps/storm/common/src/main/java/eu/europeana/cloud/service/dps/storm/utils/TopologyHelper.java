package eu.europeana.cloud.service.dps.storm.utils;

/**
 * Created by Tarek on 7/15/2016.
 */
public final class TopologyHelper {
    public static final String SPOUT = "spout";
    public static final String PARSE_TASK_BOLT = "parseTaskBolt";
    public static final String RETRIEVE_FILE_BOLT = "retrieveFileBolt";
    public static final String READ_DATASETS_BOLT = "readDatasetsBolt";
    public static final String READ_DATASET_BOLT = "readDatasetBolt";
    public static final String READ_REPRESENTATION_BOLT = "readRepresentationBolt";
    public static final String IC_BOLT = "icBolt";
    public static final String NOTIFICATION_BOLT = "notificationBolt";
    public static final String WRITE_RECORD_BOLT = "writeRecordBolt";
    public static final String XSLT_BOLT = "XSLT_BOLT";
    public static final String WRITE_TO_DATA_SET_BOLT = "writeToDataSetBolt";
    public static final String REVISION_WRITER_BOLT = "revisionWriterBolt";
    public static final String VALIDATION_BOLT = "validationBolt";
    public static final String STATISTICS_BOLT = "statisticsBolt";
    public static final String ENRICHMENT_BOLT = "enrichmentBolt";

    public static final String IDENTIFIERS_HARVESTING_BOLT = "identifiersHarvestingBolt";
    public static final String RECORD_HARVESTING_BOLT = "recordHarvestingBolt";
    public static final String TASK_SPLITTING_BOLT = "TaskSplittingBolt";
    public static final String HTTP_HARVESTING_BOLT = "HTTPHarvestingBolt";
}
