package eu.europeana.cloud.service.dps.storm;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;

/**
 * Increases progress by 1 per Task / per tuple received.
 * 
 * Progress bolt can be placed at the end of a topology,
 * to count the amount of records that are fully processed.
 * 
 * Right now only single Progress bolt is supported per Topology.
 * 
 * @author manos
 */
public class ProgressBolt extends AbstractDpsBolt {

	private static final Logger LOGGER = LoggerFactory.getLogger(ProgressBolt.class);

	private transient ZookeeperMultiCountMetric zMetric;
	private String zkAddress;
	
	public ProgressBolt(String zkAddress) {
		this.zkAddress = zkAddress;
	}

	@Override
	public void prepare() {

		initMetrics(topologyContext);
	}

	void initMetrics(TopologyContext context) {
		
		zMetric = new ZookeeperMultiCountMetric(zkAddress);
		context.registerMetric("zMetric=>", zMetric, 10);
	}

	@Override
	public void execute(StormTaskTuple t) {

		try {
			LOGGER.info("ProgressIncreaseBolt: updating progress");
			updateProgress(t);
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
		}
		outputCollector.emit(t.toStormTuple());
	}

	private void updateProgress(StormTaskTuple t) {
		
		zMetric.incr(t.getTaskId());
		LOGGER.info("ProgressIncreaseBolt: progress updated");
	}
}