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
import eu.europeana.cloud.dps.topologies.media.support.StatsTupleData.Status;
import eu.europeana.cloud.dps.topologies.media.support.Util;
import eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyPropertyKeys;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskErrorsDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;

public class StatsBolt extends BaseRichBolt {

    private static final long UPDATE_PERIOD = 1000;

    private static final Logger logger = LoggerFactory.getLogger(StatsBolt.class);

    private transient CassandraTaskInfoDAO taskInfoDao;
    private transient CassandraTaskErrorsDAO taskErrorsDao;

    private String topologyName;
    private HashMap<Long, TaskStats> taskStats = new HashMap<>();
    private HashMap<String, String> errorToType = new HashMap<>();

    private transient OutputCollector outputCollector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;

        topologyName = (String) stormConf.get(TopologyPropertyKeys.TOPOLOGY_NAME);

        CassandraConnectionProvider connectionProvider = Util.getCassandraConnectionProvider(stormConf);
        taskInfoDao = CassandraTaskInfoDAO.getInstance(connectionProvider);
        taskErrorsDao = CassandraTaskErrorsDAO.getInstance(connectionProvider);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // nothing to declare
    }

    @Override
    public void execute(Tuple tuple) {
        if (StatsInitTupleData.STREAM_ID.equals(tuple.getSourceStreamId())) {
            handleStatsInit(tuple);
        } else if (StatsTupleData.STREAM_ID.equals(tuple.getSourceStreamId())) {
            handleStats(tuple);
        } else {
            logger.error("Received tuple from unrecognized stream id: {}", tuple.getSourceStreamId());
        }
        outputCollector.ack(tuple);
    }

    private void handleStatsInit(Tuple tuple) {
        StatsInitTupleData data = (StatsInitTupleData) tuple.getValueByField(StatsInitTupleData.FIELD_NAME);
        long taskId = data.getTaskId();
        TaskStats stats = taskStats.computeIfAbsent(taskId, TaskStats::new);
        stats.init(data.getEdmCount(), data.getStartTime());

        taskInfoDao.insert(taskId, topologyName, (int) data.getEdmCount(),
                TaskState.CURRENTLY_PROCESSING.toString(), "", new Date(data.getStartTime()));
        updateDao(stats);
        logger.info("starting stats gathering for task {}", taskId);
    }

    private void handleStats(Tuple tuple) {
        StatsTupleData data = (StatsTupleData) tuple.getValueByField(StatsTupleData.FIELD_NAME);
        long taskId = data.getTaskId();
        TaskStats stats = taskStats.computeIfAbsent(taskId, TaskStats::new);
        stats.update(data);

        if (stats.processedEdmsCount == stats.totalEdmsCount) {
            taskInfoDao.endTask(taskId, (int) stats.processedEdmsCount, (int) stats.failedEdmsCount,
                    stats.toString(), TaskState.PROCESSED.toString(), new Date());
            taskStats.remove(taskId);
            logger.info("finished stats gathering for task {}", taskId);
        } else {
            updateDao(stats);
        }

        for (Status error : data.getErrors()) {
            String errorType = getErrorType(error.message);
            taskErrorsDao.updateErrorCounter(taskId, errorType);
            taskErrorsDao.insertError(taskId, errorType, error.message, error.resourceUrl, error.date.toString());
        }
    }

    private String getErrorType(String error) {
        return errorToType.computeIfAbsent(error, k -> new com.eaio.uuid.UUID().toString());
    }

    private void updateDao(TaskStats stats) {
        if (stats.lastUpdate < System.currentTimeMillis() - UPDATE_PERIOD) {
            taskInfoDao.updateTask(stats.taskId, stats.toString(), TaskState.CURRENTLY_PROCESSING.toString(),
                    new Date(stats.startTime));
            taskInfoDao.setUpdateProcessedFiles(stats.taskId, (int) stats.processedEdmsCount,
                    (int) stats.failedEdmsCount);
            stats.lastUpdate = System.currentTimeMillis();
        }
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
        long downloadTimeSum;
        long downloadedResourceCount;

        long processingTimeSum;
        long uploadTimeSum;
        long processedResourceCount;

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

            for (Status error : data.getErrors())
                errorsCount.merge(error.message, 1L, Long::sum);

            if (data.getDownloadedBytes() > 0) {
                bytesDownloaded += data.getDownloadedBytes();
                downloadStart = Math.min(downloadStart, data.getDownloadStartTime());
                downloadEnd = Math.max(downloadEnd, data.getDownloadEndTime());
                downloadTimeSum += data.getDownloadEndTime() - data.getDownloadStartTime();
                downloadedResourceCount++;
            }

            if (data.getUploadEndTime() > 0) {
                processingTimeSum += data.getProcessingEndTime() - data.getProcessingStartTime();
                uploadTimeSum += data.getUploadEndTime() - data.getUploadStartTime();
                processedResourceCount++;
            }
        }

        @Override
        public String toString() {
            JSONObject json = new JSONObject();
            json.put("resourcesTotal", totalResourcesCount);
            json.put("resourcesFailed", failedResourcesCount);
            json.put("resourcesSuccessfull", totalResourcesCount - failedEdmsCount);
            json.put("bytesDownloaded", bytesDownloaded);
            long downloadTime = (downloadEnd - downloadStart + 500) / 1000;
            if (downloadTime > 0) {
                json.put("downloadTimeSeconds", downloadTime);
                json.put("downloadSpeedMBps", (double) bytesDownloaded / downloadTime / (1024 * 1024));
                json.put("averageDownloadTimeMillis", downloadTimeSum / downloadedResourceCount);
            }
            if (processedResourceCount > 0) {
                json.put("averageProcessingTimeMillis", processingTimeSum / processedResourceCount);
                json.put("averageUploadTimeMillis", uploadTimeSum / processedResourceCount);
            }
            json.put("errors", errorsCount);
            Date now = new Date();
            json.put("lastUpdate", now);
            json.put("runningTimeSeconds", (now.getTime() - startTime + 500) / 1000);
            return json.toString();
        }
    }
}
