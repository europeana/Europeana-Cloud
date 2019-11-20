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
import java.util.List;

import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.*;

public class CassandraTasksDAO extends CassandraDAO {

    private PreparedStatement findTasksInGivenState;

    public CassandraTasksDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    @Override
    void prepareStatements() {
        findTasksInGivenState = dbService.getSession().prepare(
                "SELECT * FROM " + TASKS_BY_STATE_TABLE + " WHERE " + STATE + " = ?");
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
