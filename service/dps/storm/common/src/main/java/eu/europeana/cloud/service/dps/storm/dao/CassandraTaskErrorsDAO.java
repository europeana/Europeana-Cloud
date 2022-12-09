package eu.europeana.cloud.service.dps.storm.dao;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.DPS_DEFAULT_MAX_ATTEMPTS;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Iterators;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.dps.ErrorNotification;
import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.storm.ErrorType;
import eu.europeana.cloud.service.dps.storm.conversion.ErrorNotificationConverter;
import eu.europeana.cloud.service.dps.storm.conversion.ErrorTypeConverter;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * The {@link TaskInfo} DAO
 *
 * @author akrystian
 */
@Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
public class CassandraTaskErrorsDAO extends CassandraDAO {

  private static CassandraTaskErrorsDAO instance = null;
  private PreparedStatement insertErrorStatement;
  private PreparedStatement insertErrorCounterStatement;
  private PreparedStatement selectErrorCountsStatement;
  private PreparedStatement selectErrorCountsForErrorTypeStatement;
  private PreparedStatement removeErrorCountsStatement;
  private PreparedStatement removeErrorNotifications;
  private PreparedStatement selectErrorTypeFieldFromErrorTypeStatement;
  private PreparedStatement selectErrorTypesStatement;
  private PreparedStatement selectErrorNotificationStatement;
  private PreparedStatement selectErrorNotificationsWithGivenLimitStatement;
  private PreparedStatement selectErrorTypeStatement;


  /**
   * @param dbService The service exposing the connection and session
   */
  public CassandraTaskErrorsDAO(CassandraConnectionProvider dbService) {
    super(dbService);
  }

  public CassandraTaskErrorsDAO() {
    //needed for creating cglib proxy in RetryableMethodExecutor.createRetryProxy()
  }

  public static synchronized CassandraTaskErrorsDAO getInstance(CassandraConnectionProvider cassandra) {
    if (instance == null) {
      instance = RetryableMethodExecutor.createRetryProxy(new CassandraTaskErrorsDAO(cassandra));

    }
    return instance;
  }

  /**
   * Retrieves all errorType records from cassandra error_type table with given task_id
   *
   * @param taskId identifier of task
   * @return List of  ErrorType class instances
   */
  public List<ErrorType> getErrorTypes(long taskId) {
    List<ErrorType> errorTypes = new ArrayList<>();
    dbService.getSession().execute(selectErrorTypesStatement.bind(taskId)).forEach(
        row -> errorTypes.add(ErrorTypeConverter.fromDBRow(row))
    );
    return errorTypes;
  }

  public void insertErrorCounter(long taskId, String errorType, int number) {
    dbService.getSession().execute(insertErrorCounterStatement(taskId,
        ErrorType.builder()
                 .taskId(taskId)
                 .count(number)
                 .uuid(errorType)
                 .build()));
  }

  public BoundStatement insertErrorCounterStatement(long taskId, ErrorType errorType) {
    return insertErrorCounterStatement.bind(taskId, UUID.fromString(errorType.getUuid()), errorType.getCount());
  }

  /**
   * Insert information about the resource and its error
   *
   * @param taskId task identifier
   * @param errorType type of error
   * @param errorMessage error message
   * @param resource resource identifier
   */
  public void insertError(long taskId, String errorType, String errorMessage, String resource, String additionalInformations) {
    dbService.getSession().execute(insertErrorStatement(
        ErrorNotification.builder()
                         .taskId(taskId)
                         .errorType(errorType)
                         .errorMessage(errorMessage)
                         .resource(resource)
                         .additionalInformations(additionalInformations)
                         .build()
    ));
  }

  public BoundStatement insertErrorStatement(ErrorNotification errorNotification) {
    return insertErrorStatement.bind(
        errorNotification.getTaskId(),
        UUID.fromString(errorNotification.getErrorType()),
        errorNotification.getErrorMessage(),
        errorNotification.getResource(),
        errorNotification.getAdditionalInformations());
  }

  /**
   * Return the number of errors of all types for a given task.
   *
   * @param taskId task identifier
   * @return number of all errors for the task
   */
  public int getErrorCount(long taskId) {
    ResultSet rs = dbService.getSession().execute(selectErrorCountsStatement.bind(taskId));
    int count = 0;

    while (rs.iterator().hasNext()) {
      Row row = rs.one();

      count += row.getLong(CassandraTablesAndColumnsNames.ERROR_TYPES_COUNTER);
    }
    return count;
  }

  /**
   * Retrieves errorType record from cassandra error_type table with given task_id and error_type
   *
   * @param taskId identifier of task
   * @param errorTypeUUID error type of errors
   * @return Optional of ErrorType class instances
   */
  public Optional<ErrorType> getErrorType(long taskId, UUID errorTypeUUID) {
    ResultSet rs = dbService.getSession().execute(selectErrorTypeStatement.bind(taskId, errorTypeUUID));
    return (rs.iterator().hasNext()) ? Optional.of(ErrorTypeConverter.fromDBRow(rs.one())) : Optional.empty();
  }

  public Iterator<String> getMessagesUuids(long taskId) {
    return Iterators.transform(
        dbService.getSession().execute(selectErrorTypesStatement.bind(taskId)).iterator(),
        row -> row.getUUID(CassandraTablesAndColumnsNames.ERROR_TYPES_ERROR_TYPE).toString());
  }

  /**
   * Returns the number of errors of one given type of the error for a given task.
   *
   * @param taskId identifier of the task that will be investigated
   * @param errorType type of the error that will be used to read the counter
   * @return number of errors for the given task and given error type
   */
  public long selectErrorCountsForErrorType(long taskId, UUID errorType) {
    ResultSet rs = dbService.getSession().execute(selectErrorCountsForErrorTypeStatement.bind(taskId, errorType));
    Row result = rs.one();
    if (result != null) {
      return result.getInt(CassandraTablesAndColumnsNames.ERROR_TYPES_COUNTER);
    } else {
      return 0;
    }
  }

  public Iterator<ErrorType> getAll(long taskId) {
    return Iterators.transform(
        dbService.getSession().execute(selectErrorTypesStatement.bind(taskId)).iterator(),
        row -> ErrorType.builder()
                        .count(row.getInt(CassandraTablesAndColumnsNames.ERROR_TYPES_COUNTER))
                        .uuid(row.getUUID(CassandraTablesAndColumnsNames.ERROR_TYPES_ERROR_TYPE).toString())
                        .build());
  }

  public Optional<String> getErrorMessage(long taskId, String errorType) {
    ResultSet rs = dbService.getSession().execute(selectErrorNotificationStatement.bind(taskId, UUID.fromString(errorType)));
    if (!rs.iterator().hasNext()) {
      return Optional.empty();
    }

    String message = rs.one().getString(CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_MESSAGE);
    return Optional.of(message);
  }

  /**
   * Retrieves maximum number of errorNotification records from cassandra error_notification table with given task_id and
   * error_type.
   *
   * @param taskId identifier of task
   * @param errorTypeUid error type of errors
   * @param idsCount maximum number of records to be retrieved
   * @return List of ErrorNotification class instances
   */
  public List<ErrorNotification> getErrorNotificationsWithGivenLimit(long taskId, UUID errorTypeUid, int idsCount) {
    List<ErrorNotification> errorNotifications = new ArrayList<>();
    dbService.getSession().execute(selectErrorNotificationsWithGivenLimitStatement.bind(taskId, errorTypeUid, idsCount))
             .forEach(row ->
                 errorNotifications.add(ErrorNotificationConverter.fromDBRow(row))
             );
    return errorNotifications;
  }

  public void removeErrors(long taskId) {
    ResultSet rs = dbService.getSession().execute(selectErrorTypeFieldFromErrorTypeStatement.bind(taskId));

    while (rs.iterator().hasNext()) {
      Row row = rs.one();
      UUID errorType = row.getUUID(CassandraTablesAndColumnsNames.ERROR_TYPES_ERROR_TYPE);
      dbService.getSession().execute(removeErrorNotifications.bind(taskId, errorType));
    }
    dbService.getSession().execute(removeErrorCountsStatement.bind(taskId));
  }

  @Override
  protected void prepareStatements() {
    insertErrorStatement = dbService.getSession().prepare(
        "INSERT INTO " + CassandraTablesAndColumnsNames.ERROR_NOTIFICATIONS_TABLE
            + "("
            + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_TASK_ID + ","
            + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_TYPE + ","
            + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_MESSAGE + ","
            + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_RESOURCE + ","
            + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ADDITIONAL_INFORMATIONS + ")"
            + " VALUES (?,?,?,?,?)"
    );

    insertErrorCounterStatement = dbService.getSession().prepare(
        "INSERT INTO " + CassandraTablesAndColumnsNames.ERROR_TYPES_TABLE
            + "("
            + CassandraTablesAndColumnsNames.ERROR_TYPES_TASK_ID + ","
            + CassandraTablesAndColumnsNames.ERROR_TYPES_ERROR_TYPE + ","
            + CassandraTablesAndColumnsNames.ERROR_TYPES_COUNTER
            + ") VALUES (?,?,?)"
    );

    selectErrorCountsStatement = dbService.getSession().prepare(
        "SELECT " + CassandraTablesAndColumnsNames.ERROR_TYPES_COUNTER
            + " FROM " + CassandraTablesAndColumnsNames.ERROR_TYPES_TABLE
            + " WHERE " + CassandraTablesAndColumnsNames.ERROR_TYPES_TASK_ID + " = ? "
    );

    selectErrorCountsForErrorTypeStatement = dbService.getSession().prepare(
        "SELECT " + CassandraTablesAndColumnsNames.ERROR_TYPES_COUNTER
            + " FROM " + CassandraTablesAndColumnsNames.ERROR_TYPES_TABLE
            + " WHERE " + CassandraTablesAndColumnsNames.ERROR_TYPES_TASK_ID + " = ?"
            + " AND " + CassandraTablesAndColumnsNames.ERROR_TYPES_ERROR_TYPE + " = ?"
    );

    selectErrorTypeFieldFromErrorTypeStatement = dbService.getSession().prepare(
        "SELECT " + CassandraTablesAndColumnsNames.ERROR_TYPES_ERROR_TYPE
            + " FROM " + CassandraTablesAndColumnsNames.ERROR_TYPES_TABLE
            + " WHERE " + CassandraTablesAndColumnsNames.ERROR_TYPES_TASK_ID + " = ? "
    );

    selectErrorTypeStatement = dbService.getSession().prepare(
        String.format("select * from %s where %s = ? and %s = ?",
            CassandraTablesAndColumnsNames.ERROR_TYPES_TABLE,
            CassandraTablesAndColumnsNames.ERROR_TYPES_TASK_ID,
            CassandraTablesAndColumnsNames.ERROR_TYPES_ERROR_TYPE
        )
    );

    selectErrorTypesStatement = dbService.getSession().prepare(
        "SELECT * "
            + "FROM " + CassandraTablesAndColumnsNames.ERROR_TYPES_TABLE
            + " WHERE " + CassandraTablesAndColumnsNames.ERROR_TYPES_TASK_ID + " = ?"
    );

    selectErrorNotificationStatement = dbService.getSession().prepare(
        "SELECT * "
            + " FROM " + CassandraTablesAndColumnsNames.ERROR_NOTIFICATIONS_TABLE
            + " WHERE " + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_TASK_ID + " = ? "
            + "AND " + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_TYPE + " = ? LIMIT 1"
    );

    selectErrorNotificationsWithGivenLimitStatement = dbService.getSession().prepare(
        "SELECT * "
            + " FROM " + CassandraTablesAndColumnsNames.ERROR_NOTIFICATIONS_TABLE
            + " WHERE " + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_TASK_ID + " = ? "
            + "AND " + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_TYPE + " = ? LIMIT ?"
    );

    removeErrorCountsStatement = dbService.getSession().prepare(
        "DELETE FROM " + CassandraTablesAndColumnsNames.ERROR_TYPES_TABLE
            + " WHERE " + CassandraTablesAndColumnsNames.ERROR_TYPES_TASK_ID + " = ? "
    );

    removeErrorNotifications = dbService.getSession().prepare(
        "DELETE FROM " + CassandraTablesAndColumnsNames.ERROR_NOTIFICATIONS_TABLE
            + " WHERE " + CassandraTablesAndColumnsNames.ERROR_TYPES_TASK_ID + " = ? " +
            "AND " + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_TYPE + " = ?"
    );
  }
}
