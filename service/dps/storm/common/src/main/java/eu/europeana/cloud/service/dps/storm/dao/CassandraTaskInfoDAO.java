package eu.europeana.cloud.service.dps.storm.dao;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.common.model.dps.TaskState;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.conversion.TaskInfoConverter;
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
    private PreparedStatement updateCounters;
    private PreparedStatement updateDeleteRecordsCount;
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
    protected void prepareStatements() {
        taskSearchStatement = dbService.getSession().prepare(
                "SELECT * FROM " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE + " WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?");
        taskSearchStatement.setConsistencyLevel(dbService.getConsistencyLevel());
        updateCounters = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_RECORDS_COUNT + " = ? , " + CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_ERRORS_COUNT + " = ? WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?");
        updateCounters = dbService.getSession().prepare(
                "UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE + " SET "
                        + CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_RECORDS_COUNT + " = ? , "
                        + CassandraTablesAndColumnsNames.TASK_INFO_IGNORED_RECORDS_COUNT + " = ? , "
                        + CassandraTablesAndColumnsNames.TASK_INFO_DELETED_RECORDS_COUNT + " = ? , "
                        + CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_ERRORS_COUNT + " = ? , "
                        + CassandraTablesAndColumnsNames.TASK_INFO_DELETED_ERRORS_COUNT + " = ?" +
                        " WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?");
        updateCounters.setConsistencyLevel(dbService.getConsistencyLevel());
        updateDeleteRecordsCount = prepare("UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE
                + " SET " + CassandraTablesAndColumnsNames.TASK_INFO_DELETED_RECORDS_COUNT + " = ?" +
                " WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?");
        taskInsertStatement = dbService.getSession().prepare("INSERT INTO " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE +
                "(" + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_TOPOLOGY_NAME + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_RECORDS_NUMBER + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_RECORDS_COUNT + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_STATE + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_STATE_DESCRIPTION + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_SENT_TIMESTAMP + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_START_TIMESTAMP + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_FINISH_TIMESTAMP + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_ERRORS_COUNT + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_DEFINITION +
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?)");
        taskInsertStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        finishTask = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.TASK_INFO_STATE + " = ? , " + CassandraTablesAndColumnsNames.TASK_INFO_STATE_DESCRIPTION + " = ? , " + CassandraTablesAndColumnsNames.TASK_INFO_FINISH_TIMESTAMP + " = ? " + " WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?");
        finishTask.setConsistencyLevel(dbService.getConsistencyLevel());

        updateStatusExpectedSizeStatement = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.TASK_INFO_STATE + " = ? , " + CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_RECORDS_NUMBER + " = ?  WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?");
        updateStatusExpectedSizeStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        updateStateStatement = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE + " SET " + CassandraTablesAndColumnsNames.TASK_INFO_STATE + " = ? , " + CassandraTablesAndColumnsNames.TASK_INFO_STATE_DESCRIPTION + " = ?  WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?");
        updateStateStatement.setConsistencyLevel(dbService.getConsistencyLevel());
    }

    public Optional<TaskInfo> findById(long taskId)
            throws NoHostAvailableException, QueryExecutionException {
        return Optional.ofNullable(dbService.getSession().execute(taskSearchStatement.bind(taskId)).one()).map(TaskInfoConverter::fromDBRow);
    }

    @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS, errorMessage = "Error while inserting task")
    public void insert(long taskId, String topologyName, int expectedSize, int processedFilesCount, TaskState state,
                       String info, Date sentTime, Date startTime, Date finishTime, int errors, String taskInformations)
            throws NoHostAvailableException, QueryExecutionException {

        dbService.getSession().execute(
                taskInsertStatement.bind(taskId, topologyName, expectedSize, processedFilesCount, String.valueOf(state),
                        info, sentTime, startTime, finishTime, errors, taskInformations)
        );
    }

    public void setTaskCompletelyProcessed(long taskId, String info)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(finishTask.bind(TaskState.PROCESSED.toString(), info, new Date(), taskId));
    }

    public void setTaskDropped(long taskId, String info)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(finishTask.bind(String.valueOf(TaskState.DROPPED), info, new Date(), taskId));
    }

    public void setUpdateProcessedFiles(long taskId, int processedRecordsCount, int ignoredRecordsCount,
                                        int deletedRecordsCount, int processedErrorsCount, int deletedErrorsCount)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(updateCounters.bind(processedRecordsCount, ignoredRecordsCount,
                deletedRecordsCount, processedErrorsCount, deletedErrorsCount, taskId));
    }

    public void updateDeleteRecordsCount(long taskId, int deleteRecordsCount)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(updateDeleteRecordsCount.bind(deleteRecordsCount, taskId));
    }

    public void updateStatusExpectedSize(long taskId, TaskState state, int expectedSize)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(updateStatusExpectedSizeStatement.bind(String.valueOf(state), expectedSize, taskId));
    }

    public void updateState(long taskId, TaskState state, String info) {
        dbService.getSession().execute(updateStateStatement.bind(String.valueOf(state), info, taskId));
    }

    public boolean isDroppedTask(long taskId) throws TaskInfoDoesNotExistException {
        return (findById(taskId).orElseThrow(TaskInfoDoesNotExistException::new).getState() == TaskState.DROPPED);
    }
}
