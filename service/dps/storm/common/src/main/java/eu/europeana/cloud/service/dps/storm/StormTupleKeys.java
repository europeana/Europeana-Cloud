package eu.europeana.cloud.service.dps.storm;


/**
 * Parameters shared between Bolts and Spouts
 */
public final class StormTupleKeys {


  public static final String INPUT_FILES_TUPLE_FIELD = "INPUT_FILES";

  public static final String THROTTLING_GROUPING_ATTRIBUTE_TUPLE_FIELD = "THROTTLING_ATTRIBUTE";
  public static final String TASK_METADATA_TUPLE_FIELD = "TASK_METADATA";
  public static final String TASK_ID_TUPLE_FIELD = "TASK_ID";
  public static final String RECORD_METADATA_TUPLE_FIELD = "RECORD_METADATA";
  public static final String STORM_PROCESSING_METADATA_TUPLE_FIELD = "STORM_PROCESSING_METADATA_TUPLE_KEY";

  private StormTupleKeys() {
  }
}
