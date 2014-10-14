package eu.europeana.cloud.service.dps.examples;

import java.util.List;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import eu.europeana.cloud.client.uis.rest.CloudException;
import eu.europeana.cloud.client.uis.rest.UISClient;
import eu.europeana.cloud.common.model.DataProvider;
import eu.europeana.cloud.common.response.ResultSlice;

/**
 * Retrieves a random {@link DataProvider} from the cloud, 
 * and passes (it's name) to a Bolt.
 */
public class RandomDataProviderSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;

	private Random rand;

	private UISClient uisClient;

	/** UIS-properties for ISTI ecloud instance */
	private static final String UIS_ADDRESS = "http://146.48.82.158:8080/ecloud-service-uis-rest-0.3-SNAPSHOT";
	private static final String username = "Cristiano";
	private static final String password = "Ronaldo";

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {

		this.collector = collector;
		rand = new Random();

		uisClient = new UISClient(UIS_ADDRESS, username, password);
	}

	@Override
	public void nextTuple() {

		Utils.sleep(100);
		collector.emit(new Values(getRandomDataProvider().getId()));
	}
	
	/**
	 * @return a random DataProvider
	 */
	private DataProvider getRandomDataProvider() {

		ResultSlice<DataProvider> providersSlice = null;

		try {
			providersSlice = uisClient.getDataProviders(null);
		} catch (CloudException e) {
		}

		List<DataProvider> providers = providersSlice.getResults();

		return providers.get(rand.nextInt(providers
				.size()));
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("DATA_PROVIDER_NAME"));
	}
}