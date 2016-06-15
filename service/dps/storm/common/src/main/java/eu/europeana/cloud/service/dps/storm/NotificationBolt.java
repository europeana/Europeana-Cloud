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
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.exception.DatabaseConnectionException;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.utils.CassandraSubTaskInfoDAO;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTaskInfoDAO;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This bolt is responsible for store notifications to Cassandra.
 *
 * @author Pavel Kefurt <Pavel.Kefurt@gmail.com>
 */
public class NotificationBolt extends BaseRichBolt {
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
        try {
            NotificationTuple notificationTuple = NotificationTuple
                    .fromStormTuple(tuple);

            switch (notificationTuple.getInformationType()) {
                case UPDATE_TASK:
                    updateTask(notificationTuple.getTaskId(),
                            notificationTuple.getParameters());
                    break;
                case END_TASK:
                    endTask(notificationTuple.getTaskId(),
                            notificationTuple.getParameters());
                    break;
                case NOTIFICATION:
                    storeNotification(notificationTuple.getTaskId(),
                            notificationTuple.getParameters());
                    storeFinishState(notificationTuple.getTaskId());
                    break;
            }
        } catch (NoHostAvailableException | QueryExecutionException ex) {
            LOGGER.error("Cannot store notification to Cassandra because: {}",
                    ex.getMessage());
            return;
        } catch (Exception ex) {
            LOGGER.error("Problem with store notification because: {}",
                    ex.getMessage());
            return;
        } finally {
            outputCollector.ack(tuple);
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext tc, OutputCollector oc) {
        CassandraConnectionProvider cassandra = new CassandraConnectionProvider(hosts, port, keyspaceName,
                userName, password);
        taskInfoDAO = new CassandraTaskInfoDAO(cassandra);
        subTaskInfoDAO = new CassandraSubTaskInfoDAO(cassandra);
        topologyName = (String) stormConf.get(Config.TOPOLOGY_NAME);
        this.stormConfig = stormConf;
        this.topologyContext = tc;
        this.outputCollector = oc;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {

    }

    private void updateTask(long taskId, Map<String, Object> parameters) throws DatabaseConnectionException {
        Validate.notNull(parameters);
        String state = String.valueOf(parameters.get(NotificationParameterKeys.TASK_STATE));
        String info = String.valueOf(parameters.get(NotificationParameterKeys.INFO));
        Date startDate = prepareDate(parameters.get(NotificationParameterKeys.START_TIME));
        taskInfoDAO.updateTask(taskId, info, state, startDate);
    }


    private void endTask(long taskId, Map<String, Object> parameters) throws DatabaseConnectionException {
        Validate.notNull(parameters);
        String state = String.valueOf(parameters.get(NotificationParameterKeys.TASK_STATE));
        Date finishDate = prepareDate(parameters.get(NotificationParameterKeys.FINISH_TIME));
        String info = String.valueOf(parameters.get(NotificationParameterKeys.INFO));
        taskInfoDAO.endTask(taskId, info, state, finishDate);
    }


    private static Date prepareDate(Object dateObject) {
        Date date = null;
        if (dateObject instanceof Date)
            return (Date) dateObject;
        return date;
    }

    private void storeFinishState(long taskId) throws TaskInfoDoesNotExistException, DatabaseConnectionException {
        List<TaskInfo> task = taskInfoDAO.searchById(taskId);
        if (task != null && task.get(0) != null) {
            long count = taskInfoDAO.getProcessedCount(taskId);
            int expectedSize = task.get(0).getContainsElements();
            if (count == expectedSize) {
                taskInfoDAO.endTask(taskId, "Completely processed", String.valueOf(TaskState.PROCESSED), new Date());
            }
        }
    }

    private void storeNotification(long taskId, Map<String, Object> parameters) throws DatabaseConnectionException {
        Validate.notNull(parameters);
        String resource = String.valueOf(parameters.get(NotificationParameterKeys.RESOURCE));
        String state = String.valueOf(parameters.get(NotificationParameterKeys.STATE));
        String infoText = String.valueOf(parameters.get(NotificationParameterKeys.INFO_TEXT));
        String additionalInfo = String.valueOf(parameters.get(NotificationParameterKeys.ADDITIONAL_INFORMATIONS));
        String resultResource = String.valueOf(parameters.get(NotificationParameterKeys.RESULT_RESOURCE));
        subTaskInfoDAO.insert(taskId, topologyName, resource, state, infoText, additionalInfo, resultResource);
    }
}
