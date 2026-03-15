package eu.europeana.cloud.service.dps.storm;


/**
 * Parameters shared between Bolts and Spouts
 */
public final class StormTupleKeys {


  public static final String INPUT_FILES_TUPLE_KEY = "INPUT_FILES";

  public static final String PARAMETERS_TUPLE_KEY = "PARAMETERS";
  public static final String SOURCE_TO_HARVEST = "SOURCE";
  public static final String THROTTLING_GROUPING_ATTRIBUTE = "THROTTLING_ATTRIBUTE";
  public static final String PROCESSING_METADATA_TUPLE_KEY = "PROCESSING_METADATA";
  public static final String TASK_METADATA_TUPLE_KEY = "TASK_METADATA";
  public static final String FILE_METADATA_TUPLE_KEY = "FILE_METADATA";
  public static final String STORM_PROCESSING_METADATA_TUPLE_KEY = "STORM_PROCESSING_METADATA_TUPLE_KEY";

  private StormTupleKeys() {
  }
}
