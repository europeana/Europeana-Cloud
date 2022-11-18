package eu.europeana.cloud.service.dps.storm.dao;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.dps.TaskDiagnosticInfo;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;

import java.time.Instant;
import java.util.Date;
import java.util.Optional;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.DPS_DEFAULT_MAX_ATTEMPTS;
import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.TASK_DIAGNOSTIC_INFO_FINISH_ON_STORM_TIME;
import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.TASK_DIAGNOSTIC_INFO_ID;
import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.TASK_DIAGNOSTIC_INFO_LAST_RECORD_FINISHED_ON_STORM_TIME;
import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.TASK_DIAGNOSTIC_INFO_POST_PROCESSING_START_TIME;
import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.TASK_DIAGNOSTIC_INFO_QUEUED_TIME;
import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.TASK_DIAGNOSTIC_INFO_RECORDS_RETRY_COUNT;
import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.TASK_DIAGNOSTIC_INFO_STARTED_RECORDS_COUNT;
import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.TASK_DIAGNOSTIC_INFO_START_ON_STORM_TIME;
import static eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames.TASK_DIAGNOSTIC_INFO_TABLE;

@Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
public class TaskDiagnosticInfoDAO extends CassandraDAO {

    private static TaskDiagnosticInfoDAO instance = null;
    private PreparedStatement findByIdStatement;
    private PreparedStatement updateRecordsRetryCount;
    private PreparedStatement updateStartedRecordsCount;
    private PreparedStatement updateStartOnStormTime;
    private PreparedStatement updateFinishOnStormTime;
    private PreparedStatement updatePostprocessingStartTime;
    private PreparedStatement updateLastRecordFinishedOnStormTime;
    private PreparedStatement updateQueuedTime;


    /**
     * @param dbService The service exposing the connection and session
     */
    public TaskDiagnosticInfoDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    public TaskDiagnosticInfoDAO() {
        //needed for creating cglib proxy in RetryableMethodExecutor.createRetryProxy()
    }

    public static synchronized TaskDiagnosticInfoDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = RetryableMethodExecutor.createRetryProxy(new TaskDiagnosticInfoDAO(cassandra));
        }
        return instance;
    }


    @Override
    protected void prepareStatements() {
        findByIdStatement = prepare(String.format("SELECT * FROM %s WHERE %s = ?",
                TASK_DIAGNOSTIC_INFO_TABLE, TASK_DIAGNOSTIC_INFO_ID));
        updateStartedRecordsCount = prepareUpdateQuery(TASK_DIAGNOSTIC_INFO_STARTED_RECORDS_COUNT);
        updateRecordsRetryCount = prepareUpdateQuery(TASK_DIAGNOSTIC_INFO_RECORDS_RETRY_COUNT);
        updateQueuedTime = prepareUpdateQuery(TASK_DIAGNOSTIC_INFO_QUEUED_TIME);
        updateStartOnStormTime = prepareUpdateQuery(TASK_DIAGNOSTIC_INFO_START_ON_STORM_TIME);
        updateLastRecordFinishedOnStormTime = prepareUpdateQuery(TASK_DIAGNOSTIC_INFO_LAST_RECORD_FINISHED_ON_STORM_TIME);
        updateFinishOnStormTime = prepareUpdateQuery(TASK_DIAGNOSTIC_INFO_FINISH_ON_STORM_TIME);
        updatePostprocessingStartTime = prepareUpdateQuery(TASK_DIAGNOSTIC_INFO_POST_PROCESSING_START_TIME);

    }

    private PreparedStatement prepareUpdateQuery(String column) {
        return prepare(String.format(
                "INSERT INTO %s(%s,%s) VALUES(?,?)",
                TASK_DIAGNOSTIC_INFO_TABLE, TASK_DIAGNOSTIC_INFO_ID, column));
    }

    public Optional<TaskDiagnosticInfo> findById(long taskId)
            throws NoHostAvailableException, QueryExecutionException {
        return Optional.ofNullable(dbService.getSession().execute(findByIdStatement.bind(taskId)).one()).map(this::createTaskInfo);
    }

    public void updateRecordsRetryCount(long taskId, int retryCount)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(updateRecordsRetryCount.bind(taskId, retryCount));
    }

    public void updateStartedRecordsCount(long taskId, int startedCount)
            throws NoHostAvailableException, QueryExecutionException {
        dbService.getSession().execute(updateStartedRecordsCount.bind(taskId, startedCount));
    }

    public void updateQueuedTime(long taskId, Instant time) {
        dbService.getSession().execute(updateQueuedTime.bind(taskId, Date.from(time)));
    }

    public void updateStartOnStormTime(long taskId, Instant time) {
        dbService.getSession().execute(updateStartOnStormTime.bind(taskId, Date.from(time)));
    }

    public void updateFinishOnStormTime(long taskId, Instant time) {
        dbService.getSession().execute(updateFinishOnStormTime.bind(taskId, Date.from(time)));
    }

    public void updatePostprocessingStartTime(long taskId, Instant time) {
        dbService.getSession().execute(updatePostprocessingStartTime.bind(taskId, Date.from(time)));
    }

    public BoundStatement updateLastRecordFinishedOnStormTimeStatement(long taskId, Instant time) {
        return updateLastRecordFinishedOnStormTime.bind(taskId, Date.from(time));
    }

    public void updateLastRecordFinishedOnStormTime(long taskId, Instant time) {
        dbService.getSession().execute(updateLastRecordFinishedOnStormTimeStatement(taskId, time));
    }

    private TaskDiagnosticInfo createTaskInfo(Row row) {
        return TaskDiagnosticInfo.builder()
                .taskId(row.getLong(TASK_DIAGNOSTIC_INFO_ID))
                .startedRecordsCount(row.getInt(TASK_DIAGNOSTIC_INFO_STARTED_RECORDS_COUNT))
                .recordsRetryCount(row.getInt(TASK_DIAGNOSTIC_INFO_RECORDS_RETRY_COUNT))
                .queuedTime(getInstant(row, TASK_DIAGNOSTIC_INFO_QUEUED_TIME))
                .startOnStormTime(getInstant(row, TASK_DIAGNOSTIC_INFO_START_ON_STORM_TIME))
                .finishOnStormTime(getInstant(row, TASK_DIAGNOSTIC_INFO_FINISH_ON_STORM_TIME))
                .postProcessingStartTime(getInstant(row, TASK_DIAGNOSTIC_INFO_POST_PROCESSING_START_TIME))
                .lastRecordFinishedOnStormTime(getInstant(row, TASK_DIAGNOSTIC_INFO_LAST_RECORD_FINISHED_ON_STORM_TIME))
                .build();
    }

    private Instant getInstant(Row row, String column) {
        return Optional.ofNullable(row.getTimestamp(column)).map(Date::toInstant).orElse(null);
    }

}
