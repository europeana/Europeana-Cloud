package eu.europeana.cloud.service.dps.xslt;

import com.google.common.collect.ImmutableList;

/**
 * Parameters shared between Bolts and Spouts
 */
public final class StormTupleKeys {
	
	private StormTupleKeys() {
	}

	public static final String PARAMETERS_TUPLE_KEY = "PARAMETERS";
	
	public static final String DATASETS_TUPLE_KEY = "DATASETS";
	
	public static final String INPUT_FILES_TUPLE_KEY = "INPUT_FILES";
	
	public static final String FILE_CONTENT_TUPLE_KEY = "FILE_CONTENT";
}