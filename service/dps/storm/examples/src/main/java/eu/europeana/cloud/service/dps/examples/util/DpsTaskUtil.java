package eu.europeana.cloud.service.dps.examples.util;

import com.google.common.collect.ImmutableList;

import eu.europeana.cloud.service.dps.DpsKeys;
import eu.europeana.cloud.service.dps.DpsTask;

/**
 *  dps Task helpers
 */
public class DpsTaskUtil {
	
	// Some Crappy hardcoded string for testing
	private final static String FILE_URL = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/"
			+ "L9WSPSMVQ85/representations/edm/versions/b17c4f60-70d0-11e4-8fe1-00163eefc9c8/files/ef9322a1-5416-4109-a727-2bdfecbf352d";
	
	// This is some real xml from TEL
	private final static String XML_URL = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/L9WSPSMVQ85/"
	+ "representations/edm/versions/b17c4f60-70d0-11e4-8fe1-00163eefc9c8/files/977cdca3-cd69-44cd-819f-1e5fbfe610e3";

	/**
	 * @return a hardcoded {@link DpsTask}
	 */
	public static DpsTask generateDpsTask() {

		DpsTask task = new DpsTask();
		
		final String xsltUrl = "http://myxslt.url.com";
		
		task.addDataEntry(DpsTask.FILE_URLS, ImmutableList.of(XML_URL));
		task.addParameter(DpsKeys.XSLT_URL, xsltUrl);
		task.addParameter(DpsKeys.OUTPUT_URL, XML_URL);

		return task;
	}
}
