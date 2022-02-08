package eu.europeana.cloud.service.dps.storm;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.dps.DpsTask;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.dao.*;
import eu.europeana.cloud.service.dps.storm.notification.NotificationCacheEntry;
import eu.europeana.cloud.service.dps.storm.notification.NotificationEntryCacheBuilder;
import eu.europeana.cloud.service.dps.storm.notification.handler.NotificationHandlerConfig;
import eu.europeana.cloud.service.dps.storm.notification.handler.NotificationHandlerConfigBuilder;
import eu.europeana.cloud.service.dps.storm.notification.handler.NotificationTupleHandler;
import eu.europeana.cloud.service.dps.util.LRUCache;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

/**
 * This bolt is responsible for store notifications to Cassandra.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class NotificationBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationBolt.class);
    private final String hosts;
    private final int port;
    private final String keyspaceName;
    private final String userName;
    private final String password;
    protected transient OutputCollector outputCollector;
    protected LRUCache<Long, NotificationCacheEntry> cache = new LRUCache<>(50);

    protected String topologyName;
    private transient CassandraTaskInfoDAO taskInfoDAO;
    private transient NotificationTupleHandler notificationTupleHandler;
    private transient NotificationEntryCacheBuilder notificationEntryCacheBuilder;

    /**
     * Constructor of notification bolt.
     *
     * @param hosts        Cassandra hosts separated by comma (e.g.
     *                     localhost,192.168.47.129)
     * @param port         Cassandra port
     * @param keyspaceName Cassandra keyspace name
     * @param userName     Cassandra username
     * @param password     Cassandra password
     */
    public NotificationBolt(String hosts, int port, String keyspaceName,
                            String userName, String password) {
        this.hosts = hosts;
        this.port = port;
        this.keyspaceName = keyspaceName;
        this.userName = userName;
        this.password = password;

    }

    @Override
    public void execute(Tuple tuple) {
        try {
            var notificationTuple = NotificationTuple.fromStormTuple(tuple);
            var cachedCounters = readCachedCounters(notificationTuple);
            NotificationHandlerConfig notificationHandlerConfig =
                    NotificationHandlerConfigBuilder.prepareNotificationHandlerConfig(
                            notificationTuple,
                            cachedCounters,
                            needsPostProcessing(notificationTuple));
            notificationTupleHandler.handle(notificationTuple, notificationHandlerConfig);
        } catch (NoHostAvailableException | QueryExecutionException ex) {
            LOGGER.error("Cannot store notification to Cassandra because: {}", ex.getMessage());
        } catch (Exception ex) {
            LOGGER.error("Problem with store notification because: {}", ex.getMessage(), ex);
        } finally {
            outputCollector.ack(tuple);
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext tc, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;

        var cassandraConnectionProvider =
                CassandraConnectionProviderSingleton.getCassandraConnectionProvider(
                        hosts, port, keyspaceName, userName, password);

        taskInfoDAO = CassandraTaskInfoDAO.getInstance(cassandraConnectionProvider);
        CassandraSubTaskInfoDAO subTaskInfoDAO = CassandraSubTaskInfoDAO.getInstance(cassandraConnectionProvider);
        ProcessedRecordsDAO processedRecordsDAO = ProcessedRecordsDAO.getInstance(cassandraConnectionProvider);
        CassandraTaskErrorsDAO taskErrorDAO = CassandraTaskErrorsDAO.getInstance(cassandraConnectionProvider);
        TasksByStateDAO tasksByStateDAO = TasksByStateDAO.getInstance(cassandraConnectionProvider);
        notificationEntryCacheBuilder = new NotificationEntryCacheBuilder(subTaskInfoDAO, taskInfoDAO, taskErrorDAO);
        topologyName = (String) stormConf.get(Config.TOPOLOGY_NAME);
        notificationTupleHandler = new NotificationTupleHandler(
                processedRecordsDAO,
                TaskDiagnosticInfoDAO.getInstance(cassandraConnectionProvider),
                CassandraSubTaskInfoDAO.getInstance(cassandraConnectionProvider),
                taskErrorDAO,
                taskInfoDAO,
                tasksByStateDAO,
                BatchExecutor.getInstance(cassandraConnectionProvider),
                topologyName
        );
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        //last bolt in all topologies, nothing to declare
    }

    protected boolean needsPostProcessing(NotificationTuple tuple) throws TaskInfoDoesNotExistException, IOException {
        return false;
    }

    protected DpsTask loadDpsTask(NotificationTuple tuple) throws TaskInfoDoesNotExistException, IOException {
        Optional<TaskInfo> taskInfo = taskInfoDAO.findById(tuple.getTaskId());
        String taskDefinition = taskInfo.orElseThrow(TaskInfoDoesNotExistException::new).getDefinition();
        return new ObjectMapper().readValue(taskDefinition, DpsTask.class);
    }

    private NotificationCacheEntry readCachedCounters(NotificationTuple notificationTuple) {
        var cachedCounters = cache.get(notificationTuple.getTaskId());
        if (cachedCounters == null) {
            cachedCounters = notificationEntryCacheBuilder.build(notificationTuple.getTaskId());
            cache.put(notificationTuple.getTaskId(), cachedCounters);
        }
        return cachedCounters;
    }
}