package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.SubTaskInfo;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.service.cassandra.CassandraTablesAndColumnsNames;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@link eu.europeana.cloud.common.model.dps.TaskInfo} DAO
 *
 * @author akrystian
 */
public class CassandraTaskInfoDAO extends CassandraDAO {
    private PreparedStatement taskSearchStatement;
    private PreparedStatement taskInsertStatement;
    private CassandraSubTaskInfoDAO cassandraSubTaskInfoDAO;

    /**
     * @param dbService The service exposing the connection and session
     */
    public CassandraTaskInfoDAO(CassandraConnectionProvider dbService) {
        super(dbService);
        cassandraSubTaskInfoDAO = new CassandraSubTaskInfoDAO(dbService);
    }

    @Override
    void prepareStatements() {
        taskSearchStatement = dbService.getSession().prepare(
                "SELECT * FROM " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        taskSearchStatement.setConsistencyLevel(dbService.getConsistencyLevel());
        taskInsertStatement = dbService.getSession().prepare("INSERT INTO " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE +
                "(" + CassandraTablesAndColumnsNames.BASIC_TASK_ID + ","
                + CassandraTablesAndColumnsNames.BASIC_TOPOLOGY_NAME + ","
                + CassandraTablesAndColumnsNames.BASIC_EXPECTED_SIZE + ","
                + CassandraTablesAndColumnsNames.STATE + ","
                + CassandraTablesAndColumnsNames.INFO +
                ") VALUES (?,?,?,?,?)");
        taskInsertStatement.setConsistencyLevel(dbService.getConsistencyLevel());
    }

    public List<TaskInfo> searchById(long taskId)
            throws NoHostAvailableException, QueryExecutionException, TaskInfoDoesNotExistException {
        ResultSet rs = dbService.getSession().execute(taskSearchStatement.bind(taskId));
        if (!rs.iterator().hasNext()) {
            throw new TaskInfoDoesNotExistException();
        }
        List<TaskInfo> result = new ArrayList<>();
        for (Row row : rs.all()) {
            TaskInfo task = new TaskInfo(row.getLong(CassandraTablesAndColumnsNames.BASIC_TASK_ID), row.getString(CassandraTablesAndColumnsNames.BASIC_TOPOLOGY_NAME), TaskState.valueOf(row.getString(CassandraTablesAndColumnsNames.STATE)), row.getString(CassandraTablesAndColumnsNames.INFO));
            task.setContainsElements(row.getInt(CassandraTablesAndColumnsNames.BASIC_EXPECTED_SIZE));
            result.add(task);
        }
        return result;
    }


    public void insert(long taskId, String topologyName, int expectedSize, String state, String info)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(taskInsertStatement.bind(taskId, topologyName, expectedSize, state, info));
    }

    public List<TaskInfo> searchByIdWithSubtasks(long taskId)
            throws NoHostAvailableException, QueryExecutionException, TaskInfoDoesNotExistException {
        List<TaskInfo> result = searchById(taskId);
        for (TaskInfo taskInfo : result) {
            List<SubTaskInfo> subTasks = cassandraSubTaskInfoDAO.searchById(taskId);
            for (SubTaskInfo subTask : subTasks) {
                taskInfo.addSubtask(subTask);
            }
        }
        return result;
    }
}
