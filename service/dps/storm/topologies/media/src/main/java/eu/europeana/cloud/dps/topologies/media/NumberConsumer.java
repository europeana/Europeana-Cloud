package eu.europeana.cloud.dps.topologies.media;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.service.dps.service.zoo.ZookeeperKillService;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;

public class NumberConsumer extends AbstractDpsBolt {
	
	private static final Logger logger = LoggerFactory.getLogger(NumberConsumer.class);
	
	@Override
	public void execute(StormTaskTuple t) {
		logger.info("Number bolt " + hashCode() + " consuming number " + t.getParameter("number"));
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		
		NotificationTuple nt = NotificationTuple.prepareNotification(t.getTaskId(), t.getFileUrl(), States.SUCCESS,
				"hello", "additional", "result");
		outputCollector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
	}
	
	@Override
	public void prepare() {
		
	}
	
	public void prepare(Map stormConfig, TopologyContext tc, OutputCollector oc) {
		this.stormConfig = stormConfig;
		this.topologyContext = tc;
		this.outputCollector = oc;
		
		this.killService = new ZookeeperKillService("127.0.0.1:2181");
		prepare();
	}
	
}
