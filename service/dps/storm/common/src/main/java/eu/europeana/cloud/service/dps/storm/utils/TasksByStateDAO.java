package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import org.apache.commons.lang3.EnumUtils;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.DPS_DEFAULT_MAX_ATTEMPTS;
import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.*;

public class TasksByStateDAO extends CassandraDAO {
    private PreparedStatement insertStatement;
    private PreparedStatement deleteStatement;
    private PreparedStatement findTasksInGivenState;
    private PreparedStatement listAllInUseTopicsForTopology;
    private PreparedStatement findTask;

    public TasksByStateDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    public TasksByStateDAO() {
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

        findTask = dbService.getSession().prepare(
                "SELECT * FROM " + TASKS_BY_STATE_TABLE +
                        " WHERE " + STATE + " = ?" +
                        " AND " + TASKS_BY_STATE_TOPOLOGY_NAME + " = ?" +
                        " AND " + TASKS_BY_STATE_TASK_ID_COL_NAME + " = ?");


        findTasksInGivenState = dbService.getSession().prepare(
                "SELECT * FROM " + TASKS_BY_STATE_TABLE + " WHERE " + STATE + " IN ?");

        listAllInUseTopicsForTopology = dbService.getSession().prepare(
                "SELECT * FROM " + TASKS_BY_STATE_TABLE +
                        " WHERE " + STATE + " IN ?" +
                        " AND " + TASKS_BY_STATE_TOPOLOGY_NAME + " = ?");
    }



    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
    public void insert(String state, String topologyName, long taskId, String applicationId, String topicName, Date startTime) {
        dbService.getSession().execute(insertStatement.bind(state, topologyName, taskId, applicationId, topicName, startTime));
    }

    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
    public void delete(String state, String topologyName, long taskId) {
        dbService.getSession().execute(deleteStatement.bind(state, topologyName, taskId));
    }

    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
    public Optional<Row> findTask(String topologyName, long taskId, String oldState) {
        return Optional.ofNullable(
                dbService.getSession().execute(findTask.bind(oldState, topologyName, taskId)).one());
    }

    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
    public List<TaskInfo> findTasksInGivenState(List<TaskState> taskStates) {

        List<String> taskStatesNames = taskStates
                .stream()
                .map(Enum::toString)
                .collect(Collectors.toList());

        ResultSet rs = dbService.getSession().execute(findTasksInGivenState.bind(taskStatesNames));

        return rs.all().stream().map(this::createTaskInfo).collect(Collectors.toList());
    }

    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
    public Set<String> listAllInUseTopicsFor(String topologyName) {
        return listAllActiveTasksInTopology(topologyName).stream().map(TaskInfo::getTopicName).filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
    public List<TaskInfo> listAllActiveTasksInTopology(String topologyName) {
        ResultSet rs =
                dbService.getSession().execute(
                        listAllInUseTopicsForTopology.bind(
                                Arrays.asList(TaskState.PROCESSING_BY_REST_APPLICATION.toString()
                                        , TaskState.QUEUED.toString()), topologyName));

        return rs.all().stream().map(this::createTaskInfo).collect(Collectors.toList());
    }


    private TaskInfo createTaskInfo(Row row) {
        TaskInfo taskInfo = new TaskInfo();
        taskInfo.setId(row.getLong(TASKS_BY_STATE_TASK_ID_COL_NAME));
        taskInfo.setState(EnumUtils.getEnum(TaskState.class, row.getString(TASKS_BY_STATE_STATE_COL_NAME)));
        taskInfo.setTopologyName(row.getString(TASKS_BY_STATE_TOPOLOGY_NAME));
        taskInfo.setTopicName(row.getString(TASKS_BY_STATE_TOPIC_NAME_COL_NAME));
        taskInfo.setOwnerId(row.getString(TASKS_BY_STATE_APP_ID_COL_NAME));
        return taskInfo;
    }
}
