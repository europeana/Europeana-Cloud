
package eu.europeana.cloud.service.dps.examples;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import eu.europeana.cloud.mcs.driver.FileServiceClient;

/**
 * Stores a Record on the cloud.
 * 
 * Receives a byte array representing a Record from a tuple,
 * creates and stores a new Record on the cloud,
 * and emits the URL of the newly created record. 
 */
public class CreateRecordBolt extends BaseRichBolt {

	private OutputCollector collector;

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
    private static final String VERSION = "e91d6300-431c-11e4-8576-00163eefc9c8";
    private static final String mediaType = "text/plain";

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {

		mcsClient = new FileServiceClient(MCS_ADDRESS, username, password);
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {

		final byte[] recordBytes = tuple.getBinaryByField("RECORD_BYTES");
		final String record = new String(recordBytes);
		final String recordEdited = record + " edited by Storm!"; 
        InputStream contentStream = new ByteArrayInputStream(recordEdited.getBytes());
        
        URI uri = null;
        
		try {
			uri = mcsClient.uploadFile(CLOUD_ID, REPRESENTATION_NAME, VERSION, contentStream, mediaType);
		} 
		catch (Exception e) {
			
		}
		
		collector.emit(tuple, new Values(uri));
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("RECORD_URI"));
	}
}
