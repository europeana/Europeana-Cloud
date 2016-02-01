package eu.europeana.cloud.service.dps.storm;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.States;
import eu.europeana.cloud.service.dps.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.dps.storm.utils.CassandraSubTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * This bolt is responsible for store notifications to Cassandra.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class NotificationBolt extends BaseRichBolt {
    public static final String TaskFinishedStreamName = "FinishStream";


    private static final Logger LOGGER = LoggerFactory
            .getLogger(NotificationBolt.class);

    protected Map stormConfig;
    protected TopologyContext topologyContext;
    protected OutputCollector outputCollector;

    private final String hosts;
    private final int port;
    private final String keyspaceName;
    private final String userName;
    private final String password;
    private final Boolean grouping;

    private String topologyName;
    private Map<Long, NotificationCache> cache;

    private CassandraTaskInfoDAO taskInfoDAO;
    private CassandraSubTaskInfoDAO subTaskInfoDAO;

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
        this(hosts, port, keyspaceName, userName, password, false);
    }

    /**
     * Constructor of notification bolt.
     *
     * @param hosts        Cassandra hosts separated by comma (e.g.
     *                     localhost,192.168.47.129)
     * @param port         Cassandra port
     * @param keyspaceName Cassandra keyspace name
     * @param userName     Cassandra username
     * @param password     Cassandra password
     * @param grouping     this bolt is connected to topology by fields grouping If true:
     *                     keep number of notifications in memory and emit notification
     *                     when task is completed.
     */
    public NotificationBolt(String hosts, int port, String keyspaceName,
                            String userName, String password, Boolean grouping) {
        this.hosts = hosts;
        this.port = port;
        this.keyspaceName = keyspaceName;
        this.userName = userName;
        this.password = password;
        this.grouping = grouping;
    }

    @Override
    public void execute(Tuple tuple) {
        NotificationTuple notificationTuple = NotificationTuple
                .fromStormTuple(tuple);

        NotificationCache nCache = null;
        if (grouping) {
            nCache = cache.get(notificationTuple.getTaskId());
            if (nCache == null) {
                nCache = new NotificationCache();
                cache.put(notificationTuple.getTaskId(), nCache);
            }
        }

        try {
            switch (notificationTuple.getInformationType()) {
                case BASIC_INFO:
                    int tmp = storeBasicInfo(notificationTuple.getTaskId(),
                            notificationTuple.getParameters());
                    if (nCache != null) {
                        nCache.setTotalSize(tmp);
                    }
                    break;
                case NOTIFICATION:
                    storeNotification(notificationTuple.getTaskId(),
                            notificationTuple.getParameters());
                    if (nCache != null) {
                        nCache.inc();
                    }
                    break;
            }
        } catch (NoHostAvailableException | QueryExecutionException ex) {
            LOGGER.error("Cannot store notification to Cassandra because: {}",
                    ex.getMessage());
            outputCollector.ack(tuple);
            return;
        } catch (Exception ex) {
            LOGGER.error("Problem with store notification because: {}",
                    ex.getMessage());
            outputCollector.ack(tuple);
            return;
        }

        outputCollector.emit(notificationTuple.toStormTuple());

        // emit finish notification
        if (nCache != null && nCache.isComplete()) {
            outputCollector.emit(
                    TaskFinishedStreamName,
                    NotificationTuple.prepareNotification(
                            notificationTuple.getTaskId(), "",
                            States.FINISHED, "", "")
                            .toStormTuple());

            cache.remove(notificationTuple.getTaskId());
        }
        outputCollector.ack(tuple);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext tc, OutputCollector oc) {
        CassandraConnectionProvider cassandra = new CassandraConnectionProvider(hosts, port, keyspaceName,
                userName, password);
        taskInfoDAO = new CassandraTaskInfoDAO(cassandra);
        subTaskInfoDAO = new CassandraSubTaskInfoDAO(cassandra);
        topologyName = (String) stormConf.get(Config.TOPOLOGY_NAME);

        if (grouping) {
            cache = new HashMap<>();
        }

        this.stormConfig = stormConf;
        this.topologyContext = tc;
        this.outputCollector = oc;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
        //TODO remove one
        ofd.declare(NotificationTuple.getFields());
        ofd.declareStream(TaskFinishedStreamName, NotificationTuple.getFields());
    }

    private int storeBasicInfo(long taskId, Map<String, String> parameters) throws DatabaseConnectionException {
        Validate.notNull(parameters);
        int expectedSize = convertIfNotNull(parameters.get(NotificationParameterKeys.EXPECTED_SIZE));
        taskInfoDAO.insert(taskId, topologyName, expectedSize);
        return expectedSize;
    }

    private void storeNotification(long taskId, Map<String, String> parameters) throws DatabaseConnectionException {
        Validate.notNull(parameters);
        String resource = assignIfNotNull(parameters.get(NotificationParameterKeys.RESOURCE));
        String state = assignIfNotNull(parameters.get(NotificationParameterKeys.STATE));
        String infoText = assignIfNotNull(parameters.get(NotificationParameterKeys.INFO_TEXT));
        String additionalInfo = assignIfNotNull(parameters.get(NotificationParameterKeys.ADDITIONAL_INFORMATIONS));
        String resultResource = assignEmptyStringIfNull(parameters.get(NotificationParameterKeys.RESULT_RESOURCE));
        subTaskInfoDAO.insert(taskId, topologyName, resource, state, infoText, additionalInfo, resultResource);
    }

    private String assignIfNotNull(String input) {
        return input != null && !input.isEmpty() ? input : null;
    }

    private String assignEmptyStringIfNull(String input) {
        return input != null ? input : "";
    }


    private int convertIfNotNull(String input) {
        return input != null && !input.isEmpty() ? Integer.valueOf(input) : -1;
    }

    private class NotificationCache {
        int totalSize = -1;
        int processed = 0;

        public void setTotalSize(int totalSize) {
            this.totalSize = totalSize;
        }

        public void inc() {
            processed++;
        }

        public Boolean isComplete() {
            return totalSize != -1 ? processed >= totalSize : false;
        }
    }
}
