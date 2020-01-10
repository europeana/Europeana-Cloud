package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.*;

public class TasksByStateDAO extends CassandraDAO {
    private PreparedStatement insertStatement;
    private PreparedStatement deleteStatement;
    private PreparedStatement findTasksInGivenState;
    private PreparedStatement listAllInUseTopicsForTopology;

    public TasksByStateDAO(CassandraConnectionProvider dbService) {
        super(dbService);
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

        findTasksInGivenState = dbService.getSession().prepare(
                "SELECT * FROM " + TASKS_BY_STATE_TABLE + " WHERE " + STATE + " = ?");

        listAllInUseTopicsForTopology = dbService.getSession().prepare(
                "SELECT " + TASKS_BY_STATE_TOPIC_NAME_COL_NAME + " FROM " + TASKS_BY_STATE_TABLE +
                        " WHERE " + STATE + " IN ?" +
                        " AND " + TASKS_BY_STATE_TOPOLOGY_NAME + " = ?");
    }

    public void insert(String state, String topologyName, long taskId, String applicationId, String topicName, Date startTime)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(insertStatement.bind(state, topologyName, taskId, applicationId, topicName, startTime));
    }
    public void delete(String state, String topologyName, long taskId){
        dbService.getSession().execute(deleteStatement.bind(state, topologyName, taskId));
    }

    public void insert(String state, String topologyName, long taskId, String applicationId, String topicName) {
        insert(state, topologyName, taskId, applicationId, topicName, new Date());
    }

    public List<TaskInfo> findTasksInGivenState(TaskState taskState)
            throws NoHostAvailableException, QueryExecutionException {
        List<TaskInfo> results = new ArrayList<>();
        ResultSet rs = dbService.getSession().execute(findTasksInGivenState.bind(taskState.toString()));

        for (Row row : rs) {
            TaskInfo taskInfo = new TaskInfo();
            taskInfo.setState(TaskState.PROCESSING_BY_REST_APPLICATION);
            taskInfo.setTopologyName(row.getString(TASKS_BY_STATE_STATE_COL_NAME));
            taskInfo.setId(row.getLong(TASKS_BY_STATE_TASK_ID_COL_NAME));
            taskInfo.setOwnerId(row.getString(TASKS_BY_STATE_APP_ID_COL_NAME));
            results.add(taskInfo);
        }
        return results;
    }

    public List<String> listAllInUseTopicsFor(String topologyName) {
        List<String> results = new ArrayList<>();

        ResultSet rs =
                dbService.getSession().execute(
                        listAllInUseTopicsForTopology.bind(
                                Arrays.asList(TaskState.PROCESSING_BY_REST_APPLICATION.toString(), TaskState.CURRENTLY_PROCESSING.toString()), topologyName));

        for (Row row : rs) {
            results.add(row.getString(TASKS_BY_STATE_TOPIC_NAME_COL_NAME));
        }
        return results;
    }
}
