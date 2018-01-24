package eu.europeana.cloud.dps.topologies.media;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.dps.topologies.media.support.MediaTupleData;
import eu.europeana.cloud.dps.topologies.media.support.StatsInitTupleData;
import eu.europeana.cloud.dps.topologies.media.support.Util;
import eu.europeana.cloud.mcs.driver.DataSetServiceClient;
import eu.europeana.cloud.mcs.driver.RepresentationIterator;
import eu.europeana.cloud.service.commons.urls.UrlParser;
import eu.europeana.cloud.service.commons.urls.UrlPart;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;

public class DataSetReaderSpout extends KafkaSpout {
	
	private static final Logger logger = LoggerFactory.getLogger(DataSetReaderSpout.class);
	
	private SpoutOutputCollector outputCollector;
	
	private DataSetServiceClient datasetClient;
	
	private List<TaskInfo> pendingTasks = new ArrayList<>();
	
	private TaskInfo taskInfo;
	private RepresentationIterator representationIterator;
	private long emitLimit;
	private long emitCount = 0;
	private long startTime;
	
	public DataSetReaderSpout(Config conf) {
		super(getKafkaConfig(conf));
	}
	
	private static SpoutConfig getKafkaConfig(Config conf) {
		String topologyName = (String) conf.get(TopologyPropertyKeys.TOPOLOGY_NAME);
		ZkHosts brokerHosts = new ZkHosts((String) conf.get(TopologyPropertyKeys.INPUT_ZOOKEEPER_ADDRESS));
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topologyName, "", "storm");
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.ignoreZkOffsets = true;
		kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		return kafkaConfig;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(MediaTupleData.FIELD_NAME));
		declarer.declareStream(StatsInitTupleData.STREAM_ID, new Fields(StatsInitTupleData.FIELD_NAME));
	}
	
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		outputCollector = collector;
		
		datasetClient = Util.getDataSetServiceClient(conf);
		
		emitLimit = (Long) conf.getOrDefault("MEDIATOPOLOGY_DATASET_EMIT_LIMIT", Long.MAX_VALUE);
		
		super.open(conf, context, new CollectorWrapper(collector));
	}
	
	@Override
	public void nextTuple() {
		if (taskInfo != null && !representationIterator.hasNext() && !taskInfo.failedQueue.isEmpty()) {
			Object cloudId = taskInfo.failedQueue.remove();
			Representation rep = taskInfo.pendingRepresentations.get(cloudId);
			MediaTupleData data = new MediaTupleData(taskInfo.task.getTaskId(), rep);
			outputCollector.emit(new Values(data), rep.getCloudId());
			logger.info("Reemiting failed msg: {}", cloudId);
			return;
		}
		if (representationIterator == null || !representationIterator.hasNext()) {
			super.nextTuple();
		}
		while (representationIterator != null && representationIterator.hasNext() && emitCount < emitLimit) {
			Representation rep = representationIterator.next();
			if ("edm".equals(rep.getRepresentationName())) {
				long taskId = taskInfo.task.getTaskId();
				MediaTupleData data = new MediaTupleData(taskId, rep);
				outputCollector.emit(new Values(data), rep.getCloudId());
				taskInfo.pendingRepresentations.put(rep.getCloudId(), rep);
				
				emitCount++;
				if (!representationIterator.hasNext() || emitCount >= emitLimit) {
					taskInfo.emitComplete = true;
					outputCollector.emit(StatsInitTupleData.STREAM_ID,
							new Values(new StatsInitTupleData(taskId, startTime, emitCount)));
				}
				return;
			}
		}
		// everything retrieved
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
	
	@Override
	public void ack(Object msgId) {
		for (TaskInfo info : pendingTasks) {
			if (info.pendingRepresentations.remove(msgId) != null) {
				if (info.pendingRepresentations.isEmpty() && info.emitComplete) {
					logger.info("Task {} finished", info.task.getTaskName());
					pendingTasks.remove(info);
					super.ack(info.kafkaMsgId);
				}
				return;
			}
		}
		logger.warn("Unrecognied ACK: {}", msgId);
	}
	
	@Override
	public void fail(Object msgId) {
		for (TaskInfo info : pendingTasks) {
			Representation rep = info.pendingRepresentations.get(msgId);
			if (rep != null) {
				info.failedQueue.add(msgId);
				logger.info("FAIL received: {}", msgId);
				return;
			}
		}
		logger.warn("Unrecognized FAIL: {}", msgId);
	}
	
	private class TaskInfo {
		DpsTask task;
		Object kafkaMsgId;
		HashMap<String, Representation> pendingRepresentations = new HashMap<>();
		ArrayDeque<Object> failedQueue = new ArrayDeque<>();
		boolean emitComplete;
	}
	
	private class CollectorWrapper extends SpoutOutputCollector {
		
		public CollectorWrapper(ISpoutOutputCollector delegate) {
			super(delegate);
		}
		
		@Override
		public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
			UrlParser datasetUrlParser;
			DpsTask task;
			try {
				task = new ObjectMapper().readValue((String) tuple.get(0), DpsTask.class);
				String datasetUrl = task.getDataEntry(InputDataType.DATASET_URLS).get(0);
				datasetUrlParser = new UrlParser(datasetUrl);
			} catch (IOException e) {
				logger.error("Message '{}' rejected because: {}", tuple.get(0), e.getMessage());
				logger.debug("Exception details", e);
				return Collections.emptyList();
			}
			logger.info("Task {} parsed", task.getTaskName());
			
			taskInfo = new TaskInfo();
			taskInfo.task = task;
			taskInfo.kafkaMsgId = messageId;
			pendingTasks.add(taskInfo);
			
			emitCount = 0;
			startTime = System.currentTimeMillis();
			representationIterator = datasetClient.getRepresentationIterator(
					datasetUrlParser.getPart(UrlPart.DATA_PROVIDERS), datasetUrlParser.getPart(UrlPart.DATA_SETS));
			return Collections.emptyList();
		}
	}
}
