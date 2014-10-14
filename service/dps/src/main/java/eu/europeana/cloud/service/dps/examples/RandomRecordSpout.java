package eu.europeana.cloud.service.dps.examples;

import java.io.InputStream;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.google.common.io.ByteStreams;

import eu.europeana.cloud.mcs.driver.FileServiceClient;

/**
 * Retrieves a random record from the cloud, 
 * and passes it (as a byte array) to a Bolt.
 */
public class RandomRecordSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;

	/** MCS-properties for ISTI ecloud instance */
    private static final String MCS_ADDRESS = "http://146.48.82.158:8080/ecloud-service-mcs-rest-0.2-SNAPSHOT";
	private static final String username = "Cristiano";
	private static final String password = "Ronaldo";
	private FileServiceClient mcsClient;

	/** 
	 * Hardcoded ecloud-record properties
	 * 
	 * MUST ALREADY EXIST in the cloud
	 */
    private static final String CLOUD_ID = "W3KBLNZDKNQ";
    private static final String REPRESENTATION_NAME = "schema66";
    private static final String UPLOADED_FILE_NAME = "9007c26f-e29d-4924-9c49-8ff064484264";
    private static final String VERSION = "e91d6300-431c-11e4-8576-00163eefc9c8";
    

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {

		this.collector = collector;

		mcsClient = new FileServiceClient(MCS_ADDRESS, username, password);
	}

	@Override
	public void nextTuple() {
		
        byte[] recordBytes = getRecord();
        
		Utils.sleep(100);
		collector.emit(new Values(recordBytes));
		
	}
	
	private byte[] getRecord() {

        try {
        	
			InputStream responseStream = mcsClient.getFile(CLOUD_ID, REPRESENTATION_NAME, VERSION, UPLOADED_FILE_NAME);
	        
	        byte[] responseBytes = ByteStreams.toByteArray(responseStream);
	        return responseBytes;
	        
		} catch (Exception e) {
			
		}
        return null;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("RECORD_BYTES"));
	}
}