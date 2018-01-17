package eu.europeana.cloud.dps.topologies.media;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.dps.topologies.media.support.StatsInitTupleData;
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData;
import eu.europeana.cloud.dps.topologies.media.support.Util;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;

public class StatsBolt extends BaseRichBolt {
	
	private static final long UPDATE_PERIOD = 1000;
	
	private static final Logger logger = LoggerFactory.getLogger(StatsBolt.class);
	
	private CassandraTaskInfoDAO taskInfoDao;
	
	private String topologyName;
	private HashMap<Long, TaskStats> taskStats = new HashMap<>();
	
	private OutputCollector outputCollector;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.outputCollector = collector;
		
		topologyName = (String) stormConf.get(TopologyPropertyKeys.TOPOLOGY_NAME);
		
		CassandraConnectionProvider connectionProvider = Util.getCassandraConnectionProvider(stormConf);
		taskInfoDao = CassandraTaskInfoDAO.getInstance(connectionProvider);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// nothing to declare
	}
	
	@Override
	public void execute(Tuple tuple) {
		if (StatsInitTupleData.STREAM_ID.equals(tuple.getSourceStreamId())) {
			StatsInitTupleData data = (StatsInitTupleData) tuple.getValueByField(StatsInitTupleData.FIELD_NAME);
			long taskId = data.getTaskId();
			TaskStats stats = taskStats.computeIfAbsent(taskId, TaskStats::new);
			stats.init(data.getEdmCount(), data.getStartTime());
			
			taskInfoDao.insert(taskId, topologyName, (int) data.getEdmCount(),
					TaskState.CURRENTLY_PROCESSING.toString(), "", new Date(data.getStartTime()));
			updateDao(stats);
			logger.info("starting stats gathering for task " + taskId);
		} else if (StatsTupleData.STREAM_ID.equals(tuple.getSourceStreamId())) {
			StatsTupleData data = (StatsTupleData) tuple.getValueByField(StatsTupleData.FIELD_NAME);
			long taskId = data.getTaskId();
			TaskStats stats = taskStats.computeIfAbsent(taskId, TaskStats::new);
			stats.update(data);
			
			if (stats.processedEdmsCount == stats.totalEdmsCount) {
				taskInfoDao.endTask(taskId, (int) stats.processedEdmsCount, (int) stats.failedEdmsCount,
						stats.toString(), TaskState.PROCESSED.toString(), new Date());
				taskStats.remove(taskId);
				logger.info("finished stats gathering for task " + taskId);
			} else if (stats.lastUpdate < System.currentTimeMillis() - UPDATE_PERIOD) {
				updateDao(stats);
			}
		}
		outputCollector.ack(tuple);
	}
	
	private void updateDao(TaskStats stats) {
		taskInfoDao.updateTask(stats.taskId, stats.toString(), TaskState.CURRENTLY_PROCESSING.toString(),
				new Date(stats.startTime));
		taskInfoDao.setUpdateProcessedFiles(stats.taskId, (int) stats.processedEdmsCount,
				(int) stats.failedEdmsCount);
		stats.lastUpdate = System.currentTimeMillis();
	}
	
	private static class TaskStats {
		final long taskId;
		
		long totalEdmsCount;
		long processedEdmsCount;
		long failedEdmsCount;
		
		long totalResourcesCount;
		long failedResourcesCount;
		Map<String, Long> errorsCount = new HashMap<>();
		
		long bytesDownloaded;
		long downloadStart = Long.MAX_VALUE;
		long downloadEnd;
		
		long startTime;
		long lastUpdate;
		
		public TaskStats(long taskId) {
			this.taskId = taskId;
		}
		
		void init(long totalEdmsCount, long startTime) {
			this.totalEdmsCount = totalEdmsCount;
			this.startTime = startTime;
		}
		
		void update(StatsTupleData data) {
			processedEdmsCount++;
			if (!data.getErrors().isEmpty())
				failedEdmsCount++;
			totalResourcesCount += data.getResourceCount();
			failedResourcesCount += data.getErrors().size();
			
			for (String error : data.getErrors())
				errorsCount.merge(error, 1L, Long::sum);
			
			bytesDownloaded += data.getDownloadedBytes();
			downloadStart = Math.min(downloadStart, data.getDownloadStartTime());
			downloadEnd = Math.max(downloadEnd, data.getDownloadEndTime());
		}
		
		@Override
		public String toString() {
			JSONObject json = new JSONObject();
			json.put("resourcesTotal", totalResourcesCount);
			json.put("resourcesFailed", failedResourcesCount);
			json.put("resourcesSuccessfull", totalResourcesCount - failedEdmsCount);
			json.put("bytesDownloaded", bytesDownloaded);
			long downloadTime = (downloadEnd - downloadStart + 500) / 1000;
			json.put("downloadTimeSeconds", downloadTime);
			if (downloadTime > 0)
				json.put("downloadSpeedMBps", (double) bytesDownloaded / downloadTime / 1000000);
			json.put("errors", errorsCount);
			Date now = new Date();
			json.put("lastUpdate", now);
			json.put("runningTimeSeconds", (now.getTime() - startTime + 500) / 1000);
			return json.toString();
		}
	}
}
