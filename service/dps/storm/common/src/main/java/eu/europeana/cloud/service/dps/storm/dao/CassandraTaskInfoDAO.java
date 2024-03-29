package eu.europeana.cloud.service.dps.storm.dao;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.DPS_DEFAULT_MAX_ATTEMPTS;

import com.datastax.driver.core.BoundStatement;
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
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;
import java.util.Date;
import java.util.Optional;

/**
 * The {@link eu.europeana.cloud.common.model.dps.TaskInfo} DAO
 *
 * @author akrystian
 */
@Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
public class CassandraTaskInfoDAO extends CassandraDAO {

  private static CassandraTaskInfoDAO instance = null;
  private PreparedStatement taskSearchStatement;
  private PreparedStatement taskInsertStatement;
  private PreparedStatement updateCounters;
  private PreparedStatement finishTask;
  private PreparedStatement updateStatusExpectedSizeStatement;
  private PreparedStatement updateStateStatement;
  private PreparedStatement updateSubmitParameters;
  private PreparedStatement updatePostProcessedRecordsCount;
  private PreparedStatement updateExpectedPostProcessedRecordsNumber;

  /**
   * @param dbService The service exposing the connection and session
   */
  public CassandraTaskInfoDAO(CassandraConnectionProvider dbService) {
    super(dbService);
  }

  public CassandraTaskInfoDAO() {
    //needed for creating cglib proxy in RetryableMethodExecutor.createRetryProxy()
  }

  /**
   * Thread safe method creating instance of cassandra task info DAO if it doesn't exist or returning one if it exists.
   *
   * @param cassandra instance of cassandra cluster connection
   * @return instance of cassandra task info DAO
   */
  public static synchronized CassandraTaskInfoDAO getInstance(CassandraConnectionProvider cassandra) {
    if (instance == null) {
      instance = RetryableMethodExecutor.createRetryProxy(new CassandraTaskInfoDAO(cassandra));
    }
    return instance;
  }


  @Override
  protected void prepareStatements() {
    taskSearchStatement = dbService.getSession().prepare(
        "SELECT * "
            + "FROM " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE
            + " WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?"
    );

    updateCounters = dbService.getSession().prepare(
        "UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE
            + " SET " + CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_RECORDS_COUNT + " = ? , "
            + CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_ERRORS_COUNT + " = ?"
            + "WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?"
    );

    updateCounters = dbService.getSession().prepare(
        "UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE + " SET "
            + CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_RECORDS_COUNT + " = ? , "
            + CassandraTablesAndColumnsNames.TASK_INFO_IGNORED_RECORDS_COUNT + " = ? , "
            + CassandraTablesAndColumnsNames.TASK_INFO_DELETED_RECORDS_COUNT + " = ? , "
            + CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_ERRORS_COUNT + " = ? , "
            + CassandraTablesAndColumnsNames.TASK_INFO_DELETED_ERRORS_COUNT + " = ?" +
            " WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?"
    );

    taskInsertStatement = dbService.getSession().prepare(
        "INSERT INTO " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE
            + "("
            + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + ","
            + CassandraTablesAndColumnsNames.TASK_INFO_TOPOLOGY_NAME + ","
            + CassandraTablesAndColumnsNames.TASK_INFO_STATE + ","
            + CassandraTablesAndColumnsNames.TASK_INFO_STATE_DESCRIPTION + ","
            + CassandraTablesAndColumnsNames.TASK_INFO_SENT_TIMESTAMP + ","
            + CassandraTablesAndColumnsNames.TASK_INFO_START_TIMESTAMP + ","
            + CassandraTablesAndColumnsNames.TASK_INFO_FINISH_TIMESTAMP + ","
            + CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_RECORDS_NUMBER + ","
            + CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_RECORDS_COUNT + ","
            + CassandraTablesAndColumnsNames.TASK_INFO_IGNORED_RECORDS_COUNT + ","
            + CassandraTablesAndColumnsNames.TASK_INFO_DELETED_RECORDS_COUNT + ","
            + CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_ERRORS_COUNT + ","
            + CassandraTablesAndColumnsNames.TASK_INFO_DELETED_ERRORS_COUNT + ","
            + CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_POST_PROCESSED_RECORDS_NUMBER + ","
            + CassandraTablesAndColumnsNames.TASK_INFO_POST_PROCESSED_RECORDS_COUNT + ","
            + CassandraTablesAndColumnsNames.TASK_INFO_DEFINITION +
            ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    );

    finishTask = dbService.getSession().prepare(
        "UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE
            + " SET " + CassandraTablesAndColumnsNames.TASK_INFO_STATE + " = ? , "
            + CassandraTablesAndColumnsNames.TASK_INFO_STATE_DESCRIPTION + " = ? , "
            + CassandraTablesAndColumnsNames.TASK_INFO_FINISH_TIMESTAMP + " = ? "
            + " WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?"
    );

    updateStatusExpectedSizeStatement = dbService.getSession().prepare(
        "UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE
            + " SET " + CassandraTablesAndColumnsNames.TASK_INFO_STATE + " = ? , "
            + CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_RECORDS_NUMBER + " = ? "
            + " WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?"
    );

    updateStateStatement = dbService.getSession().prepare(
        "UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE
            + " SET " + CassandraTablesAndColumnsNames.TASK_INFO_STATE + " = ? , "
            + CassandraTablesAndColumnsNames.TASK_INFO_STATE_DESCRIPTION + " = ? "
            + " WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ? "
    );

    updateSubmitParameters = prepare(
        "UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE + " SET "
            + CassandraTablesAndColumnsNames.TASK_INFO_START_TIMESTAMP + " = ?"
            + ", " + CassandraTablesAndColumnsNames.TASK_INFO_STATE + " = ? "
            + ", " + CassandraTablesAndColumnsNames.TASK_INFO_STATE_DESCRIPTION + " = ? "
            + ", " + CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_RECORDS_NUMBER + " = ? "
            + " WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?"
    );

    updatePostProcessedRecordsCount = prepare(
        "UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE
            + " SET " + CassandraTablesAndColumnsNames.TASK_INFO_POST_PROCESSED_RECORDS_COUNT + " = ?"
            + " WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?"
    );

    updateExpectedPostProcessedRecordsNumber = prepare(
        "UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE
            + " SET " + CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_POST_PROCESSED_RECORDS_NUMBER + " = ?"
            + " WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?"
    );
  }

  /**
   * @param taskId task identifier
   * @return Optional TaskInfo instance
   * @throws NoHostAvailableException thrown when connection to cassandra could not be established
   * @throws QueryExecutionException thrown when error occurred when executing query
   */
  public Optional<TaskInfo> findById(long taskId)
      throws NoHostAvailableException, QueryExecutionException {
    return Optional.ofNullable(dbService.getSession().execute(taskSearchStatement.bind(taskId)).one())
                   .map(TaskInfoConverter::fromDBRow);
  }


  /**
   * @param taskInfo task info that will be added to database
   * @throws NoHostAvailableException thrown when connection to cassandra could not be established
   * @throws QueryExecutionException thrown when error occurred when executing query
   */
  @Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS, errorMessage = "Error while inserting task")
  public void insert(TaskInfo taskInfo)
      throws NoHostAvailableException, QueryExecutionException {

    dbService.getSession().execute(
        taskInsertStatement.bind(
            taskInfo.getId(),
            taskInfo.getTopologyName(),
            String.valueOf(taskInfo.getState()),
            taskInfo.getStateDescription(),
            taskInfo.getSentTimestamp(),
            taskInfo.getStartTimestamp(),
            taskInfo.getFinishTimestamp(),
            taskInfo.getExpectedRecordsNumber(),
            taskInfo.getProcessedRecordsCount(),
            taskInfo.getIgnoredRecordsCount(),
            taskInfo.getDeletedRecordsCount(),
            taskInfo.getProcessedErrorsCount(),
            taskInfo.getDeletedErrorsCount(),
            taskInfo.getExpectedPostProcessedRecordsNumber(),
            taskInfo.getPostProcessedRecordsCount(),
            taskInfo.getDefinition()
        ));
  }

  /**
   * Sets task state to processed
   *
   * @param taskId task identifier
   * @param info additional information regarding task state
   * @throws NoHostAvailableException thrown when connection to cassandra could not be established
   * @throws QueryExecutionException thrown when error occurred when executing query
   */
  public void setTaskCompletelyProcessed(long taskId, String info)
      throws NoHostAvailableException, QueryExecutionException {
    dbService.getSession().execute(finishTask.bind(TaskState.PROCESSED.toString(), info, new Date(), taskId));
  }

  /**
   * Sets task state to dropped
   *
   * @param taskId task identifier
   * @param info additional information regarding task state
   * @throws NoHostAvailableException thrown when connection to cassandra could not be established
   * @throws QueryExecutionException thrown when error occurred when executing query
   */
  public void setTaskDropped(long taskId, String info)
      throws NoHostAvailableException, QueryExecutionException {
    dbService.getSession().execute(finishTask.bind(String.valueOf(TaskState.DROPPED), info, new Date(), taskId));
  }

  public BoundStatement updateProcessedFilesStatement(long taskId, int processedRecordsCount, int ignoredRecordsCount,
      int deletedRecordsCount, int processedErrorsCount, int deletedErrorsCount) {
    return updateCounters.bind(processedRecordsCount, ignoredRecordsCount, deletedRecordsCount, processedErrorsCount,
        deletedErrorsCount, taskId);
  }

  public void setUpdateProcessedFiles(long taskId, int processedRecordsCount, int ignoredRecordsCount,
      int deletedRecordsCount, int processedErrorsCount, int deletedErrorsCount)
      throws NoHostAvailableException, QueryExecutionException {

    dbService.getSession().execute(updateProcessedFilesStatement(taskId, processedRecordsCount, ignoredRecordsCount,
        deletedRecordsCount, processedErrorsCount, deletedErrorsCount));
  }

  public void updatePostProcessedRecordsCount(long taskId, int postProcessedRecordsCount)
      throws NoHostAvailableException, QueryExecutionException {
    dbService.getSession().execute(updatePostProcessedRecordsCount.bind(postProcessedRecordsCount, taskId));
  }

  public void updateExpectedPostProcessedRecordsNumber(long taskId, int expectedPostProcessedRecordsNumber)
      throws NoHostAvailableException, QueryExecutionException {
    dbService.getSession().execute(updateExpectedPostProcessedRecordsNumber.bind(expectedPostProcessedRecordsNumber, taskId));
  }

  public void updateStatusExpectedSize(long taskId, TaskState state, int expectedSize)
      throws NoHostAvailableException, QueryExecutionException {
    dbService.getSession().execute(updateStatusExpectedSizeStatement.bind(String.valueOf(state), expectedSize, taskId));
  }

  public void updateState(long taskId, TaskState state, String info) {
    dbService.getSession().execute(updateStateStatement(taskId, state, info));
  }

  public BoundStatement updateStateStatement(long taskId, TaskState state, String info) {
    return updateStateStatement.bind(String.valueOf(state), info, taskId);
  }

  public boolean isDroppedTask(long taskId) throws TaskInfoDoesNotExistException {
    return (findById(taskId).orElseThrow(TaskInfoDoesNotExistException::new).getState() == TaskState.DROPPED);
  }

  public void updateSubmitParameters(SubmitTaskParameters parameters)
      throws NoHostAvailableException, QueryExecutionException {
    dbService.getSession().execute(
        updateSubmitParameters.bind(
            parameters.getTaskInfo().getStartTimestamp(),
            String.valueOf(parameters.getTaskInfo().getState()), parameters.getTaskInfo().getStateDescription(),
            parameters.getTaskInfo().getExpectedRecordsNumber(), parameters.getTask().getTaskId()
        )
    );
  }

}
