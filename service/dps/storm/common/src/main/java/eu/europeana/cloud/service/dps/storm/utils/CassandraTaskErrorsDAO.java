package eu.europeana.cloud.service.dps.storm.utils;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.dps.*;
import eu.europeana.cloud.service.dps.exception.TaskErrorsInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.exception.TaskInfoDoesNotExistException;
import eu.europeana.cloud.service.dps.service.cassandra.CassandraTablesAndColumnsNames;

import java.util.*;

/**
 * The {@link TaskInfo} DAO
 *
 * @author akrystian
 */
public class CassandraTaskErrorsDAO extends CassandraDAO {
    private PreparedStatement insertErrorStatement;
    private PreparedStatement updateErrorCounterStatement;


    private static CassandraTaskErrorsDAO instance = null;

    public static CassandraTaskErrorsDAO getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            synchronized (CassandraTaskErrorsDAO.class) {
                if (instance == null) {
                    instance = new CassandraTaskErrorsDAO(cassandra);
                }
            }
        }
        return instance;
    }


    /**
     * @param dbService The service exposing the connection and session
     */
    private CassandraTaskErrorsDAO(CassandraConnectionProvider dbService) {
        super(dbService);
    }

    @Override
    void prepareStatements() {
        insertErrorStatement = dbService.getSession().prepare("INSERT INTO " + CassandraTablesAndColumnsNames.ERROR_NOTIFICATIONS_TABLE +
                "(" + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_TASK_ID + ","
                + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_TYPE + ","
                + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_ERROR_MESSAGE + ","
                + CassandraTablesAndColumnsNames.ERROR_NOTIFICATION_RESOURCE +
                ") VALUES (?,?,?,?)");
        insertErrorStatement.setConsistencyLevel(dbService.getConsistencyLevel());

        updateErrorCounterStatement = dbService.getSession().prepare("UPDATE " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_TABLE +
                " SET " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_COUNTER + " = " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_COUNTER + " + 1 " +
                "WHERE " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_TASK_ID + " = ? " +
                "AND " + CassandraTablesAndColumnsNames.ERROR_COUNTERS_ERROR_TYPE + " = ?");
        updateErrorCounterStatement.setConsistencyLevel(dbService.getConsistencyLevel());
    }


    /**
     * Update number of errors of the given type that occurred in the given task
     *
     * @param taskId task identifier
     * @param errorType type of error
     */
    public void updateErrorCounter(long taskId, String errorType) {
        dbService.getSession().execute(updateErrorCounterStatement.bind(taskId, UUID.fromString(errorType)));
    }

    /**
     * Insert information about the resource and its error
     *
     * @param taskId task identifier
     * @param errorType type of error
     * @param errorMessage error message
     * @param resource resource identifier
     */
    public void insertError(long taskId, String errorType, String errorMessage, String resource) {
        dbService.getSession().execute(insertErrorStatement.bind(taskId, UUID.fromString(errorType), errorMessage, resource));
    }
}
