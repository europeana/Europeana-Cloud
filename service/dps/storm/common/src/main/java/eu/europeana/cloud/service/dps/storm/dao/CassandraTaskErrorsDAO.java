package eu.europeana.cloud.service.dps.storm.dao;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Iterators;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;

import eu.europeana.cloud.common.model.dps.TaskInfo;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import eu.europeana.cloud.service.dps.storm.utils.CassandraTablesAndColumnsNames;

import java.util.*;

import static eu.europeana.cloud.service.dps.storm.topologies.properties.TopologyDefaultsConstants.DPS_DEFAULT_MAX_ATTEMPTS;

/**
 * The {@link TaskInfo} DAO
 *
 * @author akrystian
 */
@Retryable(maxAttempts = DPS_DEFAULT_MAX_ATTEMPTS)
public class CassandraTaskErrorsDAO extends CassandraDAO {
    private PreparedStatement insertErrorStatement;
    private PreparedStatement updateErrorCounterStatement;
    private PreparedStatement selectErrorCountsStatement;
    private PreparedStatement selectErrorCountsForErrorTypeStatement;
    private PreparedStatement removeErrorCountsStatement;
    private PreparedStatement removeErrorNotifications;

    private PreparedStatement selectErrorTypeStatement;
    private PreparedStatement selectErrorsStatement;


    private static CassandraTaskErrorsDAO instance = null;
    private PreparedStatement selectErrorStatement;

    public static synchronized CassandraTaskErrorsDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = RetryableMethodExecutor.createRetryProxy(new CassandraTaskErrorsDAO(cassandra));

        }
        return instance;
    }


    /**
     * @param dbService The service exposing the connection and session
     */
    public CassandraTaskErrorsDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    public CassandraTaskErrorsDAO() {
        //needed for creating cglib proxy in RetryableMethodExecutor.createRetryProxy()
    }

    @Override
    void prepareStatements() {
        insertErrorStatement = dbService.getSession().prepare("INSERT INTO " + CassandraTablesAndColumnsNames.ERROR_NOTIFICATIONS_TABLE +
                "(" + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_TASK_ID + ","
                + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_TYPE + ","
                + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_MESSAGE + ","
                + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_RESOURCE + ","
                + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ADDITIONAL_INFORMATIONS +
                ") VALUES (?,?,?,?,?)");
        insertErrorStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        updateErrorCounterStatement = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_TABLE +
                " SET " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_COUNTER + " = " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_COUNTER + " + 1 " +
                "WHERE " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_ERROR_TYPE + " = ?");
        updateErrorCounterStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        selectErrorCountsStatement = dbService.getSession().prepare("SELECT " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_COUNTER +
                " FROM " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_TASK_ID + " = ? ");
        selectErrorCountsStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        selectErrorCountsForErrorTypeStatement = dbService.getSession().prepare("SELECT " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_COUNTER +
                " FROM " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_TASK_ID + " = ?" +
                " AND " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_ERROR_TYPE + " = ?");
        selectErrorCountsStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        selectErrorTypeStatement = dbService.getSession().prepare("SELECT " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_ERROR_TYPE +
                " FROM " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_TASK_ID + " = ? ");
        selectErrorTypeStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        selectErrorsStatement = dbService.getSession().prepare("SELECT * FROM " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_TASK_ID + " = ?");
        selectErrorsStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        selectErrorStatement = dbService.getSession().prepare("SELECT * FROM " + CassandraTablesAndColumnsNames.ERROR_NOTIFICATIONS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_TYPE + " = ? LIMIT 1");
        selectErrorStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        removeErrorCountsStatement = dbService.getSession().prepare("DELETE  FROM " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_TASK_ID + " = ? ");
        removeErrorCountsStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        removeErrorNotifications = dbService.getSession().prepare("DELETE  FROM " + CassandraTablesAndColumnsNames.ERROR_NOTIFICATIONS_TABLE +
                " WHERE " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_TASK_ID + " = ? and " + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_TYPE + " = ?");
        removeErrorNotifications.setConsistencyLevel(dbService.getConsistencyLevel());
    }


    /**
     * Update number of errors of the given type that occurred in the given task
     *
     * @param taskId    task identifier
     * @param errorType type of error
     */
    public void updateErrorCounter(long taskId, String errorType) {
        dbService.getSession().execute(updateErrorCounterStatement.bind(taskId, UUID.fromString(errorType)));
    }

    /**
     * Insert information about the resource and its error
     *
     * @param taskId       task identifier
     * @param errorType    type of error
     * @param errorMessage error message
     * @param resource     resource identifier
     */
    public void insertError(long taskId, String errorType, String errorMessage, String resource, String additionalInformations) {
        dbService.getSession().execute(insertErrorStatement.bind(taskId, UUID.fromString(errorType), errorMessage, resource, additionalInformations));
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

            count += row.getLong(CassandraTablesAndColumnsNames.ERROR_COUNTERS_COUNTER);
        }
        return count;
    }

    /**
     *
     * Returns the number of errors of one given type of the error for a given task.
     *
     * @param taskId identifier of the task that will be investigated
     * @param errorType type of the error that will be used to read the counter
     * @return
     */
    public long getErrorCount(long taskId, UUID errorType) {
        ResultSet rs = dbService.getSession().execute(selectErrorCountsForErrorTypeStatement.bind(taskId, errorType));
        Row result = rs.one();
        if (result != null) {
            return result.getLong(CassandraTablesAndColumnsNames.ERROR_COUNTERS_COUNTER);
        } else {
            return 0;
        }
    }

    public Iterator<String> getMessagesUuids(long taskId) {
        return Iterators.transform(
                dbService.getSession().execute(selectErrorsStatement.bind(taskId)).iterator(),
                row -> row.getUUID(CassandraTablesAndColumnsNames.ERROR_COUNTERS_ERROR_TYPE).toString());
    }

    public Optional<String> getErrorMessage(long taskId, String errorType)  {
        ResultSet rs = dbService.getSession().execute(selectErrorStatement.bind(taskId, UUID.fromString(errorType)));
        if (!rs.iterator().hasNext()) {
            return Optional.empty();
        }

        String message = rs.one().getString(CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_MESSAGE);
        return Optional.of(message);
    }

    public void removeErrors(long taskId) {
        ResultSet rs = dbService.getSession().execute(selectErrorTypeStatement.bind(taskId));

        while (rs.iterator().hasNext()) {
            Row row = rs.one();
            UUID errorType = row.getUUID(CassandraTablesAndColumnsNames.ERROR_COUNTERS_ERROR_TYPE);
            dbService.getSession().execute(removeErrorNotifications.bind(taskId, errorType));
        }
        dbService.getSession().execute(removeErrorCountsStatement.bind(taskId));
    }
}
