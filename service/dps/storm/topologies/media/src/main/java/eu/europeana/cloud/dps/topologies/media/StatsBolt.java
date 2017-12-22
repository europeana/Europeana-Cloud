package eu.europeana.cloud.dps.topologies.media;

import java.math.BigInteger;
import java.util.Date;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.service.zoo.ZookeeperKillService;
import eu.europeana.cloud.service.dps.storm.AbstractDpsBolt;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;

public class StatsBolt extends AbstractDpsBolt {
	private static final Logger logger = LoggerFactory.getLogger(FileDownloadBolt.class);
	
	private int files = 0;
	private BigInteger size = BigInteger.valueOf(0);
	private BigInteger time = BigInteger.valueOf(0);
	
	@Override
	public void execute(StormTaskTuple t) {
		size = size.add(BigInteger.valueOf(Long.parseLong(t.getParameter("length"))));
		time = time.add(BigInteger.valueOf(Long.parseLong(t.getParameter("time"))));
		files++;
		
		BigInteger averageSize = size.divide(BigInteger.valueOf(files));
		BigInteger averageSpeed = size.divide(time).multiply(BigInteger.valueOf(1000));
		BigInteger averageTime = time.divide(BigInteger.valueOf(files));
		
		Date startTime = new Date();
		
		NotificationTuple nt = null;
		if (files <= 1) {
			nt = NotificationTuple.prepareNotification(t.getTaskId(), t.getFileUrl(), States.SUCCESS,
					
					averageSize.toString() + ";" + averageSpeed.toString(),
					averageSize.toString() + ";" + averageSpeed.toString(), "result");
			
		}
		
		else {
			nt =
					NotificationTuple.prepareUpdateTask(t.getTaskId(),
							"{\"averageSize\":\"" + averageSize.toString() + "\",\"averageSpeed\":\""
									+ averageSpeed.toString() + "\",\"averageTime\":\"" + averageTime + "\"}",
							TaskState.CURRENTLY_PROCESSING,
							startTime);
			
		}
		
		outputCollector.emit(NOTIFICATION_STREAM_NAME, nt.toStormTuple());
	}
	
	@Override
	public void prepare() {
		
	}
	
	@Override
	public void prepare(Map stormConfig, TopologyContext tc, OutputCollector oc) {
		this.stormConfig = stormConfig;
		this.topologyContext = tc;
		this.outputCollector = oc;
		
		this.killService = new ZookeeperKillService("127.0.0.1:2181");
		prepare();
	}
}
