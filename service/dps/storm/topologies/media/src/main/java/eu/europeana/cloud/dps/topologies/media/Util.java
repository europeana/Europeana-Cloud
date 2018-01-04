package eu.europeana.cloud.dps.topologies.media;

import java.util.Map;

import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;

public class Util {
	
	private static final String CONF_FS_URL = "MEDIATOPOLOGY_FILE_SERVICE_URL";
	private static final String CONF_FS_USER = "MEDIATOPOLOGY_FILE_SERVICE_USER";
	private static final String CONF_FS_PASS = "MEDIATOPOLOGY_FILE_SERVICE_PASSWORD";
	
	public static DataSetServiceClient getDataSetServiceClient(Map<String, String> config) {
		return new DataSetServiceClient(config.get(CONF_FS_URL), config.get(CONF_FS_USER),
				config.get(CONF_FS_PASS));
	}
	
	public static FileServiceClient getFileServiceClient(Map<String, String> config) {
		return new FileServiceClient(config.get(CONF_FS_URL), config.get(CONF_FS_USER), config.get(CONF_FS_PASS));
	}
	
	public static RecordServiceClient getRecordServiceClient(Map<String, String> config) {
		return new RecordServiceClient(config.get(CONF_FS_URL), config.get(CONF_FS_USER), config.get(CONF_FS_PASS));
	}
}
