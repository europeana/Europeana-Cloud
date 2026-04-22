package eu.europeana.cloud.service.dps.storm.dao;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.dps.EngineTaskState;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.storm.conversion.TaskInfoConverter;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;
import eu.europeana.cloud.service.dps.storm.utils.SubmitTaskParameters;

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

  private static CassandraTaskInfoDAO instance = null;
  private PreparedStatement taskSearchStatement;
    private PreparedStatement taskSearchBackwardCompatibleStatement;
  private PreparedStatement taskInsertStatement;
  private PreparedStatement updateCounters;
  private PreparedStatement finishTask;
  private PreparedStatement updateStatusExpectedRecordsStatement;
  private PreparedStatement updateStatusExpectedRecordsAndExpectedDepublishRecordsStatement;
  private PreparedStatement updatePostProcessedRecordsAndExpectedDepublishRecordsStatement;
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

      taskSearchBackwardCompatibleStatement = dbService.getSession().prepare(
              "SELECT * "
                      + "FROM " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE_BACKWARD_COMPATIBLE
                      + " WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?"
      );


    updateCounters = dbService.getSession().prepare(
        "UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE + " SET "
                + CassandraTablesAndColumnsNames.TASK_INFO_SUCCESS_RECORDS + " = ? , "
                + CassandraTablesAndColumnsNames.TASK_INFO_WARNING_RECORDS + " = ? , "
                + CassandraTablesAndColumnsNames.TASK_INFO_FAIL_RECORDS + " = ? , "
                + CassandraTablesAndColumnsNames.TASK_INFO_DUPLICATE_RECORDS + " = ? , "
                + CassandraTablesAndColumnsNames.TASK_INFO_UNCHANGED_RECORDS + " = ? , "
                + CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_RECORDS + " = ?, "
                + CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_DEPUBLISH_RECORDS + " = ?, "
                + CassandraTablesAndColumnsNames.TASK_INFO_SUCCESS_DEPUBLISH_RECORDS + " = ?, "
                + CassandraTablesAndColumnsNames.TASK_INFO_FAIL_DEPUBLISH_RECORDS + " = ?"
                + " WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?"
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
                + CassandraTablesAndColumnsNames.TASK_INFO_SUCCESS_RECORDS + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_WARNING_RECORDS + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_FAIL_RECORDS + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_DUPLICATE_RECORDS + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_UNCHANGED_RECORDS + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_RECORDS + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_POST_PROCESSED_RECORDS + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_POST_PROCESSED_RECORDS + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_DEPUBLISH_RECORDS + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_SUCCESS_DEPUBLISH_RECORDS + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_FAIL_DEPUBLISH_RECORDS + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_PROCESSED_DEPUBLISH_RECORDS + ","
                + CassandraTablesAndColumnsNames.TASK_INFO_DEFINITION
                + ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    );

    finishTask = dbService.getSession().prepare(
        "UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE
            + " SET " + CassandraTablesAndColumnsNames.TASK_INFO_STATE + " = ? , "
            + CassandraTablesAndColumnsNames.TASK_INFO_STATE_DESCRIPTION + " = ? , "
            + CassandraTablesAndColumnsNames.TASK_INFO_FINISH_TIMESTAMP + " = ? "
            + " WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?"
    );

    updateStatusExpectedRecordsAndExpectedDepublishRecordsStatement = dbService.getSession().prepare(
            "UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE
                    + " SET " + CassandraTablesAndColumnsNames.TASK_INFO_STATE + " = ? , "
                    + CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_RECORDS + " = ? ,"
                    + CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_DEPUBLISH_RECORDS + " = ? "
                    + " WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?"
    );

    updatePostProcessedRecordsAndExpectedDepublishRecordsStatement = dbService.getSession().prepare(
            "UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE
                    + " SET " + CassandraTablesAndColumnsNames.TASK_INFO_POST_PROCESSED_RECORDS + " = ? ,"
                    + CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_DEPUBLISH_RECORDS + " = ? "
                    + " WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?"
    );

    updateStatusExpectedRecordsStatement = dbService.getSession().prepare(
            "UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE
                    + " SET " + CassandraTablesAndColumnsNames.TASK_INFO_STATE + " = ? , "
                    + CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_RECORDS + " = ? "
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
                + ", " + CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_RECORDS + " = ? "
            + " WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?"
    );

    updatePostProcessedRecordsCount = prepare(
        "UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE
                + " SET " + CassandraTablesAndColumnsNames.TASK_INFO_POST_PROCESSED_RECORDS + " = ?"
            + " WHERE " + CassandraTablesAndColumnsNames.TASK_INFO_TASK_ID + " = ?"
    );

    updateExpectedPostProcessedRecordsNumber = prepare(
        "UPDATE " + CassandraTablesAndColumnsNames.TASK_INFO_TABLE
                + " SET " + CassandraTablesAndColumnsNames.TASK_INFO_EXPECTED_POST_PROCESSED_RECORDS + " = ?"
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
      Optional<TaskInfo> taskInfoOptional = Optional.ofNullable(dbService.getSession().execute(taskSearchStatement.bind(taskId)).one())
                   .map(TaskInfoConverter::fromDBRow);
      //If result present in new table then return, otherwise check old table
      if (taskInfoOptional.isPresent()) {
          return taskInfoOptional;
      }
      return Optional.ofNullable(dbService.getSession().execute(taskSearchBackwardCompatibleStatement.bind(taskId)).one())
              .map(TaskInfoConverter::fromDBRowBackwardCompatible);
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
                String.valueOf(taskInfo.getEngineTaskState()),
                taskInfo.getEngineTaskStateInfo(),
                taskInfo.getSentTimestamp(),
                taskInfo.getStartTimestamp(),
                taskInfo.getFinishTimestamp(),
                taskInfo.getSuccessRecords(),
                taskInfo.getWarningRecords(),
                taskInfo.getFailRecords(),
                taskInfo.getDuplicateRecords(),
                taskInfo.getUnchangedRecords(),
                taskInfo.getProcessedRecords(),
                taskInfo.getPostProcessedRecords(),
                taskInfo.getExpectedPostProcessedRecords(),
                taskInfo.getExpectedDepublishRecords(),
                taskInfo.getSuccessDepublishRecords(),
                taskInfo.getFailDepublishRecords(),
                taskInfo.getProcessedDepublishRecords(),
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
    dbService.getSession().execute(finishTask.bind(EngineTaskState.PROCESSED.toString(), info, new Date(), taskId));
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
    dbService.getSession().execute(finishTask.bind(String.valueOf(EngineTaskState.DROPPED), info, new Date(), taskId));
  }

  public BoundStatement updateProcessedFilesStatement(long taskId,
                                                      int successRecords, int warningRecords, int failRecords,
                                                      int duplicateRecords, int unchangedRecords, int processedRecords,
                                                      int processedDepublish, int successDepublish, int failDepublish) {
    return updateCounters.bind(successRecords, warningRecords, failRecords, duplicateRecords,
            unchangedRecords, processedRecords, processedDepublish, successDepublish, failDepublish, taskId);
  }

  public void updatePostProcessedRecordsCount(long taskId, int postProcessedRecordsCount)
      throws NoHostAvailableException, QueryExecutionException {
    dbService.getSession().execute(updatePostProcessedRecordsCount.bind(postProcessedRecordsCount, taskId));
  }

  public void updateExpectedPostProcessedRecordsNumber(long taskId, int expectedPostProcessedRecordsNumber)
      throws NoHostAvailableException, QueryExecutionException {
    dbService.getSession().execute(updateExpectedPostProcessedRecordsNumber.bind(expectedPostProcessedRecordsNumber, taskId));
  }


  public void updateStatusExpectedRecords(long taskId, EngineTaskState state, int expectedSize)
      throws NoHostAvailableException, QueryExecutionException {
    dbService.getSession().execute(updateStatusExpectedRecordsStatement
            .bind(String.valueOf(state), expectedSize, taskId));
  }

  public void updateStatusExpectedRecordsAndExpectedDepublishSize(long taskId, EngineTaskState state, int expectedSize, int expectedDepublishSize)
          throws NoHostAvailableException, QueryExecutionException {
    dbService.getSession().execute(updateStatusExpectedRecordsAndExpectedDepublishRecordsStatement
            .bind(String.valueOf(state), expectedSize, expectedDepublishSize, taskId));
  }

  public void updatePostProcessedRecordsAndExpectedDepublishRecords(long taskId, int postProcessedRecords, int expectedDepublishSize)
          throws NoHostAvailableException, QueryExecutionException {
    dbService.getSession().execute(updatePostProcessedRecordsAndExpectedDepublishRecordsStatement
            .bind(postProcessedRecords, expectedDepublishSize, taskId));
  }

  public void updateState(long taskId, EngineTaskState state, String info) {
    dbService.getSession().execute(updateStateStatement(taskId, state, info));
  }

  public BoundStatement updateStateStatement(long taskId, EngineTaskState state, String info) {
    return updateStateStatement.bind(String.valueOf(state), info, taskId);
  }

  public boolean isDroppedTask(long taskId) throws TaskInfoDoesNotExistException {
    return (findById(taskId).orElseThrow(TaskInfoDoesNotExistException::new).getEngineTaskState() == EngineTaskState.DROPPED);
  }

  public void updateSubmitParameters(SubmitTaskParameters parameters)
      throws NoHostAvailableException, QueryExecutionException {
    dbService.getSession().execute(
        updateSubmitParameters.bind(
            parameters.getTaskInfo().getStartTimestamp(),
                String.valueOf(parameters.getTaskInfo().getEngineTaskState()), parameters.getTaskInfo().getEngineTaskStateInfo(),
                parameters.getTaskInfo().getExpectedRecords(), parameters.getTask().getTaskId()
        )
    );
  }

}
