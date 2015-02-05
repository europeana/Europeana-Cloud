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
	
	// sample XML
	private final static String SAMPLE_XML = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/"
			+ "L9WSPSMVQ85/representations/edm/versions/b17c4f60-70d0-11e4-8fe1-00163eefc9c8/files/af7d3a77-4b00-485f-832c-a33c5a3d7b56";

	// OUTPUT_URL
	private final static String OUTPUT_URL = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/L9WSPSMVQ85/"
    		+ "representations/edm/versions/b17c4f60-70d0-11e4-8fe1-00163eefc9c8";

	/**
	 * @return a hardcoded {@link DpsTask}
	 */
	public static DpsTask generateDpsTask() {

		DpsTask task = new DpsTask();
		
		// xslt hosted in ISTI (Franco Maria)
		final String xsltUrl = "http://pomino.isti.cnr.it/~nardini/eCloudTest/a0429_xslt";

		// xslt hosted in ULCC
		final String ulccXsltUrl = "http://ecloud.eanadev.org:8080/hera/sample_xslt.xslt";
		
		task.addDataEntry(DpsTask.FILE_URLS, ImmutableList.of(SAMPLE_XML));
		task.addParameter(DpsKeys.XSLT_URL, ulccXsltUrl);
		task.addParameter(DpsKeys.OUTPUT_URL, OUTPUT_URL);

		return task;
	}
}
