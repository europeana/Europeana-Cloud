package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;

import java.util.Date;
import java.util.List;

/**
 * The {@link eu.europeana.cloud.common.model.dps.TaskInfo} DAO
 *
 * @author akrystian
 */
public class CassandraTaskInfoDAO extends CassandraDAO {
    private PreparedStatement taskSearchStatement;
    private PreparedStatement taskInsertStatement;
    private PreparedStatement updateExpectedSize;
    private PreparedStatement updateTask;
    private PreparedStatement dropTask;
    private PreparedStatement endTask;
    private PreparedStatement updateProcessedFiles;
    private PreparedStatement killTask;


    private static CassandraTaskInfoDAO instance = null;

    public static synchronized CassandraTaskInfoDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = new CassandraTaskInfoDAO(cassandra);
        }
        return instance;
    }


    /**
     * @param dbService The service exposing the connection and session
     */
    private CassandraTaskInfoDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    @Override
    void prepareStatements() {
        taskSearchStatement = dbService.getSession().prepare(
                "SELECT * FROM " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        taskSearchStatement.setConsistencyLevel(dbService.getConsistencyLevel());
        updateTask = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.STATE + " = ? , " + CassandraTablesAndColumnsNames.START_TIME + " = ? , " + CassandraTablesAndColumnsNames.INFO + " =? WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        updateTask.setConsistencyLevel(dbService.getConsistencyLevel());
        updateProcessedFiles = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.PROCESSED_FILES_COUNT + " = ? , " + CassandraTablesAndColumnsNames.ERRORS + " = ? WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        updateProcessedFiles.setConsistencyLevel(dbService.getConsistencyLevel());
        endTask = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.PROCESSED_FILES_COUNT + " = ? , " + CassandraTablesAndColumnsNames.ERRORS + " = ? , " + CassandraTablesAndColumnsNames.STATE + " = ? , " + CassandraTablesAndColumnsNames.FINISH_TIME + " = ? , " + CassandraTablesAndColumnsNames.INFO + " =? WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        endTask.setConsistencyLevel(dbService.getConsistencyLevel());
        taskInsertStatement = dbService.getSession().prepare("INSERT INTO " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE +
                "(" + CassandraTablesAndColumnsNames.BASIC_TASK_ID + ","
                + CassandraTablesAndColumnsNames.BASIC_TOPOLOGY_NAME + ","
                + CassandraTablesAndColumnsNames.BASIC_EXPECTED_SIZE + ","
                + CassandraTablesAndColumnsNames.PROCESSED_FILES_COUNT + ","
                + CassandraTablesAndColumnsNames.STATE + ","
                + CassandraTablesAndColumnsNames.INFO + ","
                + CassandraTablesAndColumnsNames.SENT_TIME + ","
                + CassandraTablesAndColumnsNames.START_TIME + ","
                + CassandraTablesAndColumnsNames.FINISH_TIME + ","
                + CassandraTablesAndColumnsNames.ERRORS +
                ") VALUES (?,?,?,?,?,?,?,?,?,?)");
        taskInsertStatement.setConsistencyLevel(dbService.getConsistencyLevel());
        killTask = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.STATE + " = ? , " + CassandraTablesAndColumnsNames.INFO + " = ? , " + CassandraTablesAndColumnsNames.FINISH_TIME + " = ? " + " WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        killTask.setConsistencyLevel(dbService.getConsistencyLevel());
        updateExpectedSize = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.BASIC_EXPECTED_SIZE + " = ?  WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        updateExpectedSize.setConsistencyLevel(dbService.getConsistencyLevel());

        dropTask = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.STATE + " = ? , " + CassandraTablesAndColumnsNames.INFO + " =? WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        dropTask.setConsistencyLevel(dbService.getConsistencyLevel());

    }

    public TaskInfo searchById(long taskId)
            throws NoHostAvailableException, QueryExecutionException, TaskInfoDoesNotExistException {
        ResultSet rs = dbService.getSession().execute(taskSearchStatement.bind(taskId));
        if (!rs.iterator().hasNext()) {
            throw new TaskInfoDoesNotExistException();
        }
        Row row = rs.one();
        TaskInfo task = new TaskInfo(
                row.getLong(CassandraTablesAndColumnsNames.BASIC_TASK_ID),
                row.getString(CassandraTablesAndColumnsNames.BASIC_TOPOLOGY_NAME),
                TaskState.valueOf(row.getString(CassandraTablesAndColumnsNames.STATE)),
                row.getString(CassandraTablesAndColumnsNames.INFO),
                row.getTimestamp(CassandraTablesAndColumnsNames.SENT_TIME),
                row.getTimestamp(CassandraTablesAndColumnsNames.START_TIME),
                row.getTimestamp(CassandraTablesAndColumnsNames.FINISH_TIME));
        task.setExpectedSize(row.getInt(CassandraTablesAndColumnsNames.BASIC_EXPECTED_SIZE));
        task.setProcessedElementCount(row.getInt(CassandraTablesAndColumnsNames.PROCESSED_FILES_COUNT));
        return task;
    }


    public void insert(long taskId, String topologyName, int expectedSize, int processedFilesCount, String state, String info, Date sentTime, Date startTime, Date finishTime, int errors)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(taskInsertStatement.bind(taskId, topologyName, expectedSize, processedFilesCount, state, info, sentTime, startTime, finishTime, errors));
    }

    public void updateTask(long taskId, String info, String state, Date startDate)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(updateTask.bind(state, startDate, info, taskId));
    }

    public void dropTask(long taskId, String info, String state)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(dropTask.bind(state, info, taskId));
    }

    public void setUpdateExpectedSize(long taskId, int expectedSize)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(updateExpectedSize.bind(expectedSize, taskId));
    }

    public void endTask(long taskId, int processeFilesCount, int errors, String info, String state, Date finishDate)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(endTask.bind(processeFilesCount, errors, state, finishDate, info, taskId));
    }

    public void insert(long taskId, String topologyName, int expectedSize, String state, String info, Date sentTime)
            throws NoHostAvailableException, QueryExecutionException {
        insert(taskId, topologyName, expectedSize, 0, state, info, sentTime, null, null, 0);
    }

    public void startProgress(long taskId, String topologyName, int expectedSize, String state, String info, Date sentTime)
            throws NoHostAvailableException, QueryExecutionException {
        insert(taskId, topologyName, expectedSize, 0, state, info, sentTime, null, null, 0);
    }

    public void setUpdateProcessedFiles(long taskId, int processedFilesCount, int errors)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(updateProcessedFiles.bind(processedFilesCount, errors, taskId));

    }

    public void killTask(long taskId) {
        dbService.getSession().execute(killTask.bind(String.valueOf(TaskState.DROPPED), "Dropped by the user", new Date(), taskId));
    }

    public boolean hasKillFlag(long taskId) {
        String state = getTaskStatus(taskId);
        if (state.equals(String.valueOf(TaskState.DROPPED)))
            return true;
        return false;
    }

    private String getTaskStatus(long taskId) {
        ResultSet rs = dbService.getSession().execute(taskSearchStatement.bind(taskId));
        Row row = rs.one();
        return row.getString(CassandraTablesAndColumnsNames.STATE);
    }
}
