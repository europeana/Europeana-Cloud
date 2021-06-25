package eu.europeana.cloud.service.dps.storm.dao;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import org.apache.commons.lang3.EnumUtils;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.DPS_DEFAULT_MAX_ATTEMPTS;
import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.*;

@Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
public class TasksByStateDAO extends CassandraDAO {
    private static TasksByStateDAO instance;

    private PreparedStatement insertStatement;
    private PreparedStatement deleteStatement;
    private PreparedStatement findTasksByStateStatement;
    private PreparedStatement findTasksByStateAndTopologyStatement;
    private PreparedStatement findTaskByStateAndTopologyStatement;
    private PreparedStatement findTaskStatement;

    public static synchronized TasksByStateDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = RetryableMethodExecutor.createRetryProxy(new TasksByStateDAO(cassandra));
        }
        return instance;
    }

    public TasksByStateDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    public TasksByStateDAO() {
        //needed for creating cglib proxy in RetryableMethodExecutor.createRetryProxy()
    }

    @Override
    void prepareStatements() {
        insertStatement = dbService.getSession().prepare("INSERT INTO " + TASKS_BY_STATE_TABLE +
                "("
                + TASKS_BY_STATE_STATE_COL_NAME + ","
                + TASKS_BY_STATE_TOPOLOGY_NAME + ","
                + TASKS_BY_STATE_TASK_ID_COL_NAME + ","
                + TASKS_BY_STATE_APP_ID_COL_NAME + ","
                + TASKS_BY_STATE_TOPIC_NAME_COL_NAME + ","
                + TASKS_BY_STATE_START_TIME +
                ") VALUES (?,?,?,?,?,?)");

        deleteStatement = dbService.getSession().prepare("DELETE FROM " + TASKS_BY_STATE_TABLE +
                " WHERE " + STATE + " = ?" +
                " AND " + TASKS_BY_STATE_TOPOLOGY_NAME + " = ?" +
                " AND " + TASKS_BY_STATE_TASK_ID_COL_NAME + " = ?");

        findTaskStatement = dbService.getSession().prepare(
                "SELECT * FROM " + TASKS_BY_STATE_TABLE +
                        " WHERE " + STATE + " = ?" +
                        " AND " + TASKS_BY_STATE_TOPOLOGY_NAME + " = ?" +
                        " AND " + TASKS_BY_STATE_TASK_ID_COL_NAME + " = ?");


        findTasksByStateStatement = dbService.getSession().prepare(
                "SELECT * FROM " + TASKS_BY_STATE_TABLE + " WHERE " + STATE + " IN ?");


        findTasksByStateAndTopologyStatement = dbService.getSession().prepare(
                "SELECT * FROM " + TASKS_BY_STATE_TABLE +
                        " WHERE " + STATE + " IN ?" +
                        " AND " + TASKS_BY_STATE_TOPOLOGY_NAME + " = ?");

        findTaskByStateAndTopologyStatement = dbService.getSession().prepare(
                "SELECT * FROM " + TASKS_BY_STATE_TABLE +
                        " WHERE " + STATE + " IN ?" +
                        " AND " + TASKS_BY_STATE_TOPOLOGY_NAME + " = ?" +
                        " LIMIT 1");

    }



    public void insert(String state, String topologyName, long taskId, String applicationId, String topicName, Date startTime) {
        dbService.getSession().execute(insertStatement.bind(state, topologyName, taskId, applicationId, topicName, startTime));
    }

    public void delete(String state, String topologyName, long taskId) {
        dbService.getSession().execute(deleteStatement.bind(state, topologyName, taskId));
    }

    public Optional<Row> findTask(String topologyName, long taskId, String oldState) {
        return Optional.ofNullable(
                dbService.getSession().execute(findTaskStatement.bind(oldState, topologyName, taskId)).one());
    }

    public List<TaskInfo> findTasksByState(List<TaskState> taskStates) {
        ResultSet rs = dbService.getSession().execute(findTasksByStateStatement.bind(
                taskStates.stream().map(Enum::toString).collect(Collectors.toList())
        ));

        return rs.all().stream().map(this::createTaskInfo).collect(Collectors.toList());
    }

    public List<TaskInfo> findTasksByStateAndTopology(List<TaskState> taskStates, String topologyName) {
        ResultSet rs = dbService.getSession().execute(
                        findTasksByStateAndTopologyStatement.bind(
                                taskStates.stream().map(Enum::toString).collect(Collectors.toList()), topologyName));

        return rs.all().stream().map(this::createTaskInfo).collect(Collectors.toList());
    }

    public Optional<TaskInfo> findTaskByStateAndTopology(List<TaskState> taskStates, String topologyName) {
        return Optional.ofNullable(
                dbService.getSession().execute(
                        findTaskByStateAndTopologyStatement.bind(
                                taskStates.stream().map(Enum::toString).collect(Collectors.toList()), topologyName)).one())
                .map(this::createTaskInfo);
    }


    private TaskInfo createTaskInfo(Row row) {
        var taskInfo = new TaskInfo();
        taskInfo.setId(row.getLong(TASKS_BY_STATE_TASK_ID_COL_NAME));
        taskInfo.setState(EnumUtils.getEnum(TaskState.class, row.getString(TASKS_BY_STATE_STATE_COL_NAME)));
        taskInfo.setTopologyName(row.getString(TASKS_BY_STATE_TOPOLOGY_NAME));
        taskInfo.setTopicName(row.getString(TASKS_BY_STATE_TOPIC_NAME_COL_NAME));
        taskInfo.setOwnerId(row.getString(TASKS_BY_STATE_APP_ID_COL_NAME));
        return taskInfo;
    }
}
