package eu.europeana.cloud.service.dps.storm.io;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.web.ParamConstants;
import eu.europeana.cloud.mcs.driver.FileServiceClient;
import eu.europeana.cloud.mcs.driver.RecordServiceClient;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.mcs.exception.MCSException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Stores a Record on the cloud.
 * 
 * Receives a byte array representing a Record from a tuple, creates and stores
 * a new Record on the cloud, and emits the URL of the newly created record.
 */
public class WriteRecordBolt extends AbstractDpsBolt {

	private String ecloudMcsAddress;
	private String username;
	private String password;

	private FileServiceClient mcsClient;
	private RecordServiceClient recordServiceClient;

	private static final String mediaType = "text/plain";
	
	private static final String DEFAULT_REPRESENTATION_NAME="storm_new_representation";
	
	public static final Logger LOGGER = LoggerFactory.getLogger(WriteRecordBolt.class);

	public WriteRecordBolt(String ecloudMcsAddress, String username,
			String password) {

		this.ecloudMcsAddress = ecloudMcsAddress;
		this.username = username;
		this.password = password;
	}

	@Override
	public void prepare() {

		mcsClient = new FileServiceClient(ecloudMcsAddress, username, password);
		recordServiceClient = new RecordServiceClient(ecloudMcsAddress, username, password);
	}

	@Override
	public void execute(StormTaskTuple t) {
		Map<String,String> urlParams =  FileServiceClient.parseFileUri(t.getFileUrl());
		
		try {
			LOGGER.info("WriteRecordBolt: persisting...");

			final String record = t.getFileByteData();
			String outputUrl = t.getParameter(PluginParameterKeys.OUTPUT_URL);
			
			if (outputUrl == null) {
				// in case OUTPUT_URL is not provided use a random one, using the input URL as the base 
				outputUrl = t.getFileUrl();
				outputUrl = StringUtils.substringBefore(outputUrl, "/files");
				
				LOGGER.info("WriteRecordBolt: OUTPUT_URL is not provided");
			}
			LOGGER.info("WriteRecordBolt: OUTPUT_URL: {}", outputUrl);
			
			URI uri = uploadFileInNewRepresentation(t);
			
			LOGGER.info("WriteRecordBolt: file modified, new URI:" + uri);

			outputCollector.emit(t.toStormTuple());

		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		}
	}
	
	private URI uploadFileInNewRepresentation(StormTaskTuple stormTaskTuple) throws MCSException {
		Map<String,String> urlParams =  FileServiceClient.parseFileUri(stormTaskTuple.getFileUrl());
	
		String newRepresentationName = null;
		if(newRepresentationNameProvided(stormTaskTuple)){
			newRepresentationName = stormTaskTuple.getParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME);
		}else{
			newRepresentationName = DEFAULT_REPRESENTATION_NAME;
		}
		Representation rep = recordServiceClient.getRepresentation(urlParams.get(ParamConstants.P_CLOUDID), urlParams.get(ParamConstants.P_REPRESENTATIONNAME), urlParams.get(ParamConstants.P_VER));
		URI newRepresentation = recordServiceClient.createRepresentation(urlParams.get(ParamConstants.P_CLOUDID), newRepresentationName, rep.getDataProvider());
		String newRepresentationVersion = findRepresentationVersion(newRepresentation);

		URI newFileUri = mcsClient.uploadFile(newRepresentation.toString(), stormTaskTuple.getFileByteDataAsStream(), "text/xml");

		recordServiceClient.persistRepresentation(urlParams.get(ParamConstants.P_CLOUDID), newRepresentationName, newRepresentationVersion);

		return newFileUri;
	}
	
	private boolean newRepresentationNameProvided(StormTaskTuple stormTaskTuple){
		if (stormTaskTuple.getParameter(PluginParameterKeys.NEW_REPRESENTATION_NAME) != null) {
			return true;
		}else{
			return false;
		}
	}
	
	private String findRepresentationVersion(URI uri) throws MCSException {
		Pattern p = Pattern.compile(".*/records/([^/]+)/representations/([^/]+)/versions/([^/]+)");
		Matcher m = p.matcher(uri.toString());
		
		if(m.find()){
			return m.group(3);
		}else{
			throw new MCSException("Unable to find representation version in representation URL");
		}
	}
}
