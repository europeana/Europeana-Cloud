package eu.europeana.cloud.service.dps.storm;


/**
 * Parameters shared between Bolts and Spouts
 */
public final class StormTupleKeys {


  public static final String INPUT_FILES_TUPLE_FIELD = "INPUT_FILES";

  public static final String THROTTLING_GROUPING_ATTRIBUTE_TUPLE_FIELD = "THROTTLING_ATTRIBUTE";
  public static final String TASK_DATA_TUPLE_FIELD = "TASK_DATA";
  public static final String TASK_ID_TUPLE_FIELD = "TASK_ID";
  public static final String RECORD_DATA_TUPLE_FIELD = "RECORD_DATA";
  public static final String CLOUD_ID_TUPLE_FIELD = "CLOUD_ID";
  public static final String STORM_PROCESSING_DATA_TUPLE_FIELD = "STORM_PROCESSING_DATA";

  private StormTupleKeys() {
  }
}
