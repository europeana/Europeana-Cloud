package eu.europeana.cloud.service.dps.storm.topologies.oaipmh.spout;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.RecordState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.DpsRecord;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.InputDataType;
import eu.europeana.cloud.service.dps.PluginParameterKeys;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.NotificationTuple;
import eu.europeana.cloud.service.dps.storm.StormTaskTuple;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.ProcessedRecordsDAO;
import eu.europeana.cloud.service.dps.storm.utils.TaskStatusChecker;
import eu.europeana.cloud.service.dps.storm.utils.TopologiesNames;
import eu.europeana.cloud.service.dps.util.LRUCache;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static eu.europeana.cloud.service.dps.storm.AbstractDpsBolt.NOTIFICATION_STREAM_NAME;


public class ECloudSpout extends KafkaSpout {

    private static final Logger LOGGER = LoggerFactory.getLogger(ECloudSpout.class);

    private String hosts;
    private int port;
    private String keyspaceName;
    private String userName;
    private String password;

    protected CassandraTaskInfoDAO cassandraTaskInfoDAO;
    protected TaskStatusChecker taskStatusChecker;
    protected ProcessedRecordsDAO processedRecordsDAO;

    public ECloudSpout(KafkaSpoutConfig kafkaSpoutConfig, String hosts, int port, String keyspaceName,
                       String userName, String password) {
        super(kafkaSpoutConfig);

        this.hosts = hosts;
        this.port = port;
        this.keyspaceName = keyspaceName;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, new ECloudOutputCollector(collector));
        //
        CassandraConnectionProvider cassandraConnectionProvider =
                CassandraConnectionProviderSingleton.getCassandraConnectionProvider(
                        this.hosts,
                        this.port,
                        this.keyspaceName,
                        this.userName,
                        this.password);
        cassandraTaskInfoDAO = CassandraTaskInfoDAO.getInstance(cassandraConnectionProvider);
        TaskStatusChecker.init(cassandraConnectionProvider);
        taskStatusChecker = TaskStatusChecker.getTaskStatusChecker();
        processedRecordsDAO = ProcessedRecordsDAO.getInstance(cassandraConnectionProvider);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(StormTaskTuple.getFields());
        declarer.declareStream(NOTIFICATION_STREAM_NAME, NotificationTuple.getFields());
    }

    private TaskInfo findTaskInDb(long taskId) throws TaskInfoDoesNotExistException {
        return cassandraTaskInfoDAO.searchById(taskId);
    }

    public class ECloudOutputCollector extends SpoutOutputCollector {

        private LRUCache<Long, TaskInfo> cache = new LRUCache<Long, TaskInfo>(50);

        public ECloudOutputCollector(ISpoutOutputCollector delegate) {
            super(delegate);
        }

        @Override
        public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
            DpsRecord message = null;
            try {
                message = parseMessage(tuple.get(4).toString());
                if (taskStatusChecker.hasKillFlag(message.getTaskId())) {
                    LOGGER.info("Dropping kafka message because task was dropped: {}", message.getTaskId());
                    return Collections.emptyList();
                }
                TaskInfo taskInfo = prepareTaskInfo(message);
                StormTaskTuple stormTaskTuple = prepareTaskForEmission(taskInfo, message);
                updateRecordStatus(message);
                return super.emit(streamId, stormTaskTuple.toStormTuple(), messageId);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            } catch (TaskInfoDoesNotExistException e) {
                LOGGER.error("Task definision not found in DB: {}", message.getTaskId());
                return Collections.emptyList();
            }
        }

        private DpsRecord parseMessage(String rawMessage) throws IOException {
            return new ObjectMapper().readValue(rawMessage, DpsRecord.class);
        }

        private TaskInfo prepareTaskInfo(DpsRecord message) throws TaskInfoDoesNotExistException {
            TaskInfo taskInfo = findTaskInCache(message);
            //
            if (taskFoundInCache(taskInfo)) {
                LOGGER.debug("TaskInfo found in cache");
            } else {
                LOGGER.debug("TaskInfo NOT found in cache");
                taskInfo = readTaskFromDB(message.getTaskId());
                cache.put(message.getTaskId(), taskInfo);
            }
            return taskInfo;
        }

        private TaskInfo findTaskInCache(DpsRecord kafkaMessage) {
            return cache.get(kafkaMessage.getTaskId());
        }

        private boolean taskFoundInCache(TaskInfo taskInfo) {
            return taskInfo != null;
        }

        private TaskInfo readTaskFromDB(long taskId) throws TaskInfoDoesNotExistException {
            return findTaskInDb(taskId);
        }

        private StormTaskTuple prepareTaskForEmission(TaskInfo taskInfo, DpsRecord dpsRecord) throws IOException {
            //
            DpsTask dpsTask = new ObjectMapper().readValue(taskInfo.getTaskDefinition(), DpsTask.class);
            StormTaskTuple stormTaskTuple = new StormTaskTuple(
                    dpsTask.getTaskId(),
                    dpsTask.getTaskName(),
                    dpsTask.getDataEntry(InputDataType.REPOSITORY_URLS).get(0),
                    null,
                    dpsTask.getParameters(),
                    dpsTask.getOutputRevision(),
                    dpsTask.getHarvestingDetails());
            //
            stormTaskTuple.addParameter(PluginParameterKeys.CLOUD_LOCAL_IDENTIFIER, dpsRecord.getRecordId());
            stormTaskTuple.addParameter(PluginParameterKeys.SCHEMA_NAME, dpsRecord.getMetadataPrefix());
            stormTaskTuple.addParameter(PluginParameterKeys.DPS_TASK_INPUT_DATA, stormTaskTuple.getFileUrl());
            //
            return stormTaskTuple;
        }

        private void updateRecordStatus(DpsRecord message) {
            LOGGER.info("Updating record status for {} {}", message.getTaskId(), message.getRecordId());
            processedRecordsDAO.insert(
                    message.getTaskId(),
                    message.getRecordId(),
                    "",
                    TopologiesNames.OAI_TOPOLOGY,
                    RecordState.PROCESSED_BY_SPOUT.name(),
                    "",
                    "");
        }
    }
}
