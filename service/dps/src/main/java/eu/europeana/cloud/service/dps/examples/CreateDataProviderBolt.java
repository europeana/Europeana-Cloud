
package eu.europeana.cloud.service.dps.examples;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.DataProviderProperties;

/**
 * Stores a DataProvider on the cloud.
 * 
 * Receives a DataProvider name from a tuple,
 * creates and stores a new DataProvider with that name,
 * and emits the URL of the newly created provider. 
 */
public class CreateDataProviderBolt extends BaseRichBolt {

	private OutputCollector collector;

	/** UIS-properties for ISTI ecloud instance */
	private static final String UIS_ADDRESS = "http://146.48.82.158:8080/ecloud-service-uis-rest-0.3-SNAPSHOT";
	private static final String username = "Cristiano";
	private static final String password = "Ronaldo";
	
	/** Hard-coded DataProvider properties (all of the data-providers we create will have the same properties) */
	private static final DataProviderProperties DP_PROPERTIES = new DataProviderProperties("Name1",
			"Address", "website", "url", "url", "url", "person", "remarks");

	private UISClient uisClient;

	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {

		uisClient = new UISClient(UIS_ADDRESS, username, password);
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {

		final String newProviderName = tuple.getStringByField("DATA_PROVIDER_NAME") + "storm !!!";
		String newDataproviderUrl = null;


		try {
			newDataproviderUrl = uisClient.createProvider(newProviderName, DP_PROPERTIES);
		} catch (CloudException e) {
		}

		// emit the newly created DataProvider URL (as "DATA_PROVIDER_URL") 
		collector.emit(tuple, new Values(newDataproviderUrl));
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("DATA_PROVIDER_URL"));
	}
}
