package eu.europeana.cloud.service.dps.util;

import com.google.common.collect.ImmutableList;

import eu.europeana.cloud.service.dps.DpsKeys;
import eu.europeana.cloud.service.dps.DpsTask;

/**
 *  dps Task helpers
 */
public class DpsTaskUtil {

	/**
	 * @return a hardcoded {@link DpsTask}
	 */
	public static DpsTask generateDpsTask() {

		DpsTask task = new DpsTask();
		
		final String fileUrl = "http://ecloud.eanadev.org:8080/ecloud-service-mcs-rest-0.3-SNAPSHOT/records/"
				+ "L9WSPSMVQ85/representations/edm/versions/b17c4f60-70d0-11e4-8fe1-00163eefc9c8/files/ef9322a1-5416-4109-a727-2bdfecbf352d";
		
		final String xsltUrl = "http://myxslt.url.com";
		
		task.addDataEntry(DpsTask.FILE_URLS, ImmutableList.of(fileUrl));
		task.addParameter(DpsKeys.XSLT_URL, xsltUrl);
		task.addParameter(DpsKeys.OUTPUT_URL, fileUrl);

		return task;
	}
}
