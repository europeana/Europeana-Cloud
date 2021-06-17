package eu.europeana.cloud.service.dps.storm.dao;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;

import java.util.Date;
import java.util.Optional;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.DPS_DEFAULT_MAX_ATTEMPTS;

/**
 * The {@link eu.europeana.cloud.common.model.dps.TaskInfo} DAO
 *
 * @author akrystian
 */
@Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
public class CassandraTaskInfoDAO extends CassandraDAO {
    private PreparedStatement taskSearchStatement;
    private PreparedStatement taskInsertStatement;
    private PreparedStatement updateExpectedSize;
    private PreparedStatement updateTask;
    private PreparedStatement endTask;
    private PreparedStatement updateProcessedFiles;
    private PreparedStatement updateRetryCount;
    private PreparedStatement finishTask;
    private PreparedStatement updateStatusExpectedSizeStatement;
    private PreparedStatement updateStateStatement;

    private static CassandraTaskInfoDAO instance = null;

    public static synchronized CassandraTaskInfoDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = RetryableMethodExecutor.createRetryProxy(new CassandraTaskInfoDAO(cassandra));
        }
        return instance;
    }

    /**
     * @param dbService The service exposing the connection and session
     */
    public CassandraTaskInfoDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    public CassandraTaskInfoDAO() {
        //needed for creating cglib proxy in RetryableMethodExecutor.createRetryProxy()
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
        updateRetryCount = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.RETRY_COUNT + " = ? WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        updateRetryCount.setConsistencyLevel(dbService.getConsistencyLevel());
        endTask = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.PROCESSED_FILES_COUNT + " = ? , " + CassandraTablesAndColumnsNames.ERRORS + " = ? , " + CassandraTablesAndColumnsNames.STATE + " = ? , " + CassandraTablesAndColumnsNames.FINISH_TIME + " = ? , " + CassandraTablesAndColumnsNames.INFO + " =? WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        endTask.setConsistencyLevel(dbService.getConsistencyLevel());
        taskInsertStatement = dbService.getSession().prepare("INSERT INTO " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE +
                "(" + CassandraTablesAndColumnsNames.BASIC_TASK_ID + ","
                + CassandraTablesAndColumnsNames.BASIC_TOPOLOGY_NAME + ","
                + CassandraTablesAndColumnsNames.BASIC_EXPECTED_SIZE + ","
                + CassandraTablesAndColumnsNames.PROCESSED_FILES_COUNT + ","
                + CassandraTablesAndColumnsNames.RETRY_COUNT + ","
                + CassandraTablesAndColumnsNames.STATE + ","
                + CassandraTablesAndColumnsNames.INFO + ","
                + CassandraTablesAndColumnsNames.SENT_TIME + ","
                + CassandraTablesAndColumnsNames.START_TIME + ","
                + CassandraTablesAndColumnsNames.FINISH_TIME + ","
                + CassandraTablesAndColumnsNames.ERRORS + ","
                + CassandraTablesAndColumnsNames.TASK_INFORMATIONS +
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");
        taskInsertStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        finishTask = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.STATE + " = ? , " + CassandraTablesAndColumnsNames.INFO + " = ? , " + CassandraTablesAndColumnsNames.FINISH_TIME + " = ? " + " WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        finishTask.setConsistencyLevel(dbService.getConsistencyLevel());

        updateExpectedSize = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.BASIC_EXPECTED_SIZE + " = ?  WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        updateExpectedSize.setConsistencyLevel(dbService.getConsistencyLevel());

        updateStatusExpectedSizeStatement = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.STATE + " = ? , " + CassandraTablesAndColumnsNames.BASIC_EXPECTED_SIZE + " = ?  WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        updateStatusExpectedSizeStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        updateStateStatement = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.BASIC_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.STATE + " = ? , " + CassandraTablesAndColumnsNames.INFO + " = ?  WHERE " + CassandraTablesAndColumnsNames.BASIC_TASK_ID + " = ?");
        updateStateStatement.setConsistencyLevel(dbService.getConsistencyLevel());
    }

    public Optional<TaskInfo> findById(long taskId)
            throws NoHostAvailableException, QueryExecutionException {
        return Optional.ofNullable(dbService.getSession().execute(taskSearchStatement.bind(taskId)).one()).map(this::createTaskInfo);
    }

    private TaskInfo createTaskInfo(Row row) {
        TaskInfo task = new TaskInfo(
                row.getLong(CassandraTablesAndColumnsNames.BASIC_TASK_ID),
                row.getString(CassandraTablesAndColumnsNames.BASIC_TOPOLOGY_NAME),
                TaskState.valueOf(row.getString(CassandraTablesAndColumnsNames.STATE)),
                row.getString(CassandraTablesAndColumnsNames.INFO),
                row.getTimestamp(CassandraTablesAndColumnsNames.SENT_TIME),
                row.getTimestamp(CassandraTablesAndColumnsNames.START_TIME),
                row.getTimestamp(CassandraTablesAndColumnsNames.FINISH_TIME)
        );
        task.setExpectedSize(row.getInt(CassandraTablesAndColumnsNames.BASIC_EXPECTED_SIZE));
        task.setProcessedElementCount(row.getInt(CassandraTablesAndColumnsNames.PROCESSED_FILES_COUNT));
        task.setRetryCount(row.getInt(CassandraTablesAndColumnsNames.RETRY_COUNT));
        task.setTaskDefinition(row.getString(CassandraTablesAndColumnsNames.TASK_INFORMATIONS));
        task.setErrors(row.getInt(CassandraTablesAndColumnsNames.ERRORS));
        return task;
    }

    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS, errorMessage = "Error while inserting task")
    public void insert(long taskId, String topologyName, int expectedSize, int processedFilesCount, String state, String info, Date sentTime, Date startTime, Date finishTime, int errors, String taskInformations)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(taskInsertStatement.bind(taskId, topologyName, expectedSize, processedFilesCount, 0, state, info, sentTime, startTime, finishTime, errors, taskInformations));
    }

    public void updateTask(long taskId, String info, String state, Date startDate)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(updateTask.bind(String.valueOf(state), startDate, info, taskId));
    }

    public void setTaskCompletelyProcessed(long taskId, String info)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(finishTask.bind(TaskState.PROCESSED.toString(), info, new Date(), taskId));
    }

    public void setTaskDropped(long taskId, String info)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(finishTask.bind(String.valueOf(TaskState.DROPPED), info, new Date(), taskId));
    }

    public void setUpdateExpectedSize(long taskId, int expectedSize)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(updateExpectedSize.bind(expectedSize, taskId));
    }

    public void endTask(long taskId, int processeFilesCount, int errors, String info, String state, Date finishDate)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(endTask.bind(processeFilesCount, errors, String.valueOf(state), finishDate, info, taskId));
    }

    public void setUpdateProcessedFiles(long taskId, int processedFilesCount, int errors)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(updateProcessedFiles.bind(processedFilesCount, errors, taskId));
    }

    public void updateRetryCount(long taskId, int retryCount)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(updateRetryCount.bind(retryCount, taskId));
    }

    public void updateStatusExpectedSize(long taskId, String state, int expectedSize)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(updateStatusExpectedSizeStatement.bind(String.valueOf(state), expectedSize, taskId));
    }

    public void updateState(long taskId, TaskState state, String info) {
        dbService.getSession().execute(updateStateStatement.bind(String.valueOf(state), info, taskId));
    }

    public boolean hasKillFlag(long taskId) throws TaskInfoDoesNotExistException {
        String state = getTaskStatus(taskId);
        return state.equals(String.valueOf(TaskState.DROPPED));
    }

    private String getTaskStatus(long taskId) throws TaskInfoDoesNotExistException {
        return findTaskStatus(taskId)
                .orElseThrow(TaskInfoDoesNotExistException::new);
    }

    public Optional<String> findTaskStatus(long taskId) {
        return findById(taskId)
                .map(row -> row.getState().toString());
    }

}
