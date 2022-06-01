package eu.europeana.cloud.service.dps.storm;


/**
 * Parameters shared between Bolts and Spouts
 */
public final class StormTupleKeys {

    private StormTupleKeys() {
	}

	// e.g: "54698435894"
	public static final String TASK_ID_TUPLE_KEY = "TASK_ID";
	
	//e.g: "xslt-transformation-task"
	public static final String TASK_NAME_TUPLE_KEY = "TASK_NAME";
	
	public static final String DATASETS_TUPLE_KEY = "DATASETS";
	
	public static final String INPUT_FILES_TUPLE_KEY = "INPUT_FILES";

	public static final String FILE_CONTENT_TUPLE_KEY = "FILE_CONTENT";
	
	public static final String PARAMETERS_TUPLE_KEY = "PARAMETERS";

	public static final String DPS_TASK_INPUT_DATA = "DPS_TASK_INPUT_DATA";

	public static final String REVISIONS = "REVISIONS";

	public static final String SOURCE_TO_HARVEST = "SOURCE";

	public static final String RECORD_ATTEMPT_NUMBER = "RECORD_ATTEMPT_NUMBER";

	public static final String THROTTLING_GROUPING_ATTRIBUTE = "THROTTLING_ATTRIBUTE";
}