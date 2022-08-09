package eu.europeana.cloud.service.dps.storm;

import eu.europeana.cloud.cassandra.CassandraConnectionProviderSingleton;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.commons.utils.BatchExecutor;
import eu.europeana.cloud.service.dps.storm.dao.*;
import eu.europeana.cloud.service.dps.storm.notification.NotificationCacheEntry;
import eu.europeana.cloud.service.dps.storm.notification.NotificationEntryCacheBuilder;
import eu.europeana.cloud.service.dps.storm.notification.handler.NotificationHandlerConfig;
import eu.europeana.cloud.service.dps.storm.notification.handler.NotificationHandlerConfigBuilder;
import eu.europeana.cloud.service.dps.storm.notification.handler.NotificationTupleHandler;
import eu.europeana.cloud.service.dps.storm.utils.DiagnosticContextWrapper;
import eu.europeana.cloud.service.dps.util.LRUCache;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static eu.europeana.cloud.common.model.dps.TaskInfo.UNKNOWN_EXPECTED_RECORDS_NUMBER;

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
    private transient NotificationTupleHandler notificationTupleHandler;
    private transient NotificationEntryCacheBuilder notificationEntryCacheBuilder;
    private transient BatchExecutor batchExecutor;

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
        var notificationTuple = NotificationTuple.fromStormTuple(tuple);
        try {
            LOGGER.debug("{} Performing execute on tuple {}", getClass().getName(), notificationTuple);
            prepareDiagnosticContext(notificationTuple);
            var cachedCounters = readCachedCounters(notificationTuple);
            NotificationHandlerConfig notificationHandlerConfig =
                    NotificationHandlerConfigBuilder.prepareNotificationHandlerConfig(notificationTuple, cachedCounters);
            notificationTupleHandler.handle(notificationTuple, notificationHandlerConfig);
        } catch (Exception ex) {
            LOGGER.error("Cannot store notification to Cassandra because: {}", ex.getMessage(), ex);
            batchExecutor.executeAll(
                    notificationTupleHandler.prepareStatementsForTupleContainingLastRecord(
                            notificationTuple,
                            TaskState.DROPPED,
                            ex.getMessage()));
        } finally {
            outputCollector.ack(tuple);
            clearDiagnosticContext();
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext tc, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;

        var cassandraConnectionProvider =
                CassandraConnectionProviderSingleton.getCassandraConnectionProvider(
                        hosts, port, keyspaceName, userName, password);

        CassandraTaskInfoDAO taskInfoDAO = CassandraTaskInfoDAO.getInstance(cassandraConnectionProvider);
        NotificationsDAO subTaskInfoDAO = NotificationsDAO.getInstance(cassandraConnectionProvider);
        ProcessedRecordsDAO processedRecordsDAO = ProcessedRecordsDAO.getInstance(cassandraConnectionProvider);
        CassandraTaskErrorsDAO taskErrorDAO = CassandraTaskErrorsDAO.getInstance(cassandraConnectionProvider);
        TasksByStateDAO tasksByStateDAO = TasksByStateDAO.getInstance(cassandraConnectionProvider);
        notificationEntryCacheBuilder = new NotificationEntryCacheBuilder(subTaskInfoDAO, taskInfoDAO, taskErrorDAO);
        batchExecutor = BatchExecutor.getInstance(cassandraConnectionProvider);
        topologyName = (String) stormConf.get(Config.TOPOLOGY_NAME);
        notificationTupleHandler = new NotificationTupleHandler(
                processedRecordsDAO,
                TaskDiagnosticInfoDAO.getInstance(cassandraConnectionProvider),
                NotificationsDAO.getInstance(cassandraConnectionProvider),
                taskErrorDAO,
                taskInfoDAO,
                tasksByStateDAO,
                batchExecutor,
                topologyName
        );
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        //last bolt in all topologies, nothing to declare
    }

    private NotificationCacheEntry readCachedCounters(NotificationTuple notificationTuple) {
        var cachedCounters = cache.get(notificationTuple.getTaskId());
        if (cachedCounters == null) {
            cachedCounters = notificationEntryCacheBuilder.build(notificationTuple.getTaskId());
            cache.put(notificationTuple.getTaskId(), cachedCounters);
        } else {
            cachedCounters = updateExpectedRecordsNumberIfNeeded(cachedCounters, notificationTuple.getTaskId());
            cache.put(notificationTuple.getTaskId(), cachedCounters);
        }
        return cachedCounters;
    }

    private NotificationCacheEntry updateExpectedRecordsNumberIfNeeded(NotificationCacheEntry cachedCounters, long taskId) {
        if (cachedCounters.getExpectedRecordsNumber() == UNKNOWN_EXPECTED_RECORDS_NUMBER) {
            return notificationEntryCacheBuilder.build(taskId);
        }
        return cachedCounters;
    }

    private void prepareDiagnosticContext(NotificationTuple stormTaskTuple) {
        DiagnosticContextWrapper.putValuesFrom(stormTaskTuple);
    }

    private void clearDiagnosticContext() {
        DiagnosticContextWrapper.clear();
    }
}