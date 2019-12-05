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
import java.util.Date;
import java.util.List;

import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.*;

public class TasksByStateDAO extends CassandraDAO {
    private PreparedStatement findTasksInGivenState;
    private PreparedStatement insertStatement;

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
                + TASKS_BY_STATE_START_TIME +
                ") VALUES (?,?,?,?,?)");

        findTasksInGivenState = dbService.getSession().prepare(
                "SELECT * FROM " + TASKS_BY_STATE_TABLE + " WHERE " + STATE + " = ?");
    }

    public void insert(String state, String topologyName, long taskId, String applicationId, Date startTime)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(insertStatement.bind(state, topologyName, taskId, applicationId, startTime));
    }

    public void insert(String state, String topologyName, long taskId, String applicationId) {
        insert(state, topologyName, taskId, applicationId, new Date());
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
}
