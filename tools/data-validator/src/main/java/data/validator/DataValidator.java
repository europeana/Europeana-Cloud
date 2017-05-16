package data.validator;

import com.datastax.driver.core.*;

import static data.validator.constants.Constants.*;

import data.validator.cql.CQLBuilder;
import data.validator.cql.CassandraHelper;
import data.validator.jobs.RowsValidatorJob;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/**
 * Created by Tarek on 4/26/2017.
 */


public class DataValidator {
    private CassandraConnectionProvider sourceCassandraConnectionProvider;
    private CassandraConnectionProvider targetCassandraConnectionProvider;
    final static Logger LOGGER = Logger.getLogger(DataValidator.class);


    private static final String SELECT_COLUMN_NAMES = "SELECT " + COLUMN_NAME_SELECTOR + ", " + COLUMN_INDEX_TYPE + " FROM " + SYSTEM_SCHEMA_COLUMNS_TABLE +
            "  WHERE " + KEYSPACE_NAME_LABEL + "=  ?  AND " + TABLE_NAME_LABEL + "= ? ;";


    public DataValidator(CassandraConnectionProvider sourceCassandraConnectionProvider, CassandraConnectionProvider targetCassandraConnectionProvider) {
        this.sourceCassandraConnectionProvider = sourceCassandraConnectionProvider;
        this.targetCassandraConnectionProvider = targetCassandraConnectionProvider;
    }

    public void validate(String sourceTableName, String targetTableName, int threadsCount) {
        Session session = null;
        Session targetSession = null;
        ExecutorService executorService = null;
        try {
            long progressCounter = 0l;
            executorService = Executors.newFixedThreadPool(threadsCount);
            session = sourceCassandraConnectionProvider.getSession();
            List<String> primaryKeys = CassandraHelper.getPrimaryKeysNames(sourceCassandraConnectionProvider, sourceTableName, SELECT_COLUMN_NAMES);
            ResultSet rs = CassandraHelper.getPrimaryKeysFromSourceTable(sourceCassandraConnectionProvider, sourceTableName, primaryKeys);
            Iterator<Row> iterator = rs.iterator();

            targetSession = targetCassandraConnectionProvider.getSession();
            BoundStatement matchingBoundStatement = CassandraHelper.prepareBoundStatementForMatchingTargetTable(targetCassandraConnectionProvider, targetTableName, primaryKeys);
            List<Row> rows = new ArrayList<>();

            while (iterator.hasNext()) {
                Row row = iterator.next();
                rows.add(row);
                progressCounter++;
                if (progressCounter % PROGRESS_COUNTER == 0) {
                    executeTheRowsJob(targetSession, executorService, primaryKeys, matchingBoundStatement, rows);
                    LOGGER.info("The data was matched properly for " + progressCounter + " records  and the progress will continue for source table " + sourceTableName + " and target table " + targetTableName + " ....");
                    rows.clear();
                }
            }
            if (!rows.isEmpty()) {
                executeTheRowsJob(targetSession, executorService, primaryKeys, matchingBoundStatement, rows);
            }
            LOGGER.info("The data For for source table " + sourceTableName + " and target table " + targetTableName + " was validated correctly!");
        } catch (Exception e) {
            LOGGER.error("ERROR happened: " + e.getMessage() + " and The data for source table " + sourceTableName + " and target table " + targetTableName + " was NOT validated properly!");
        } finally {
            if (session != null)
                session.close();
            if (targetSession != null)
                targetSession.close();
            sourceCassandraConnectionProvider.closeConnections();
            targetCassandraConnectionProvider.closeConnections();
            if (executorService != null)
                executorService.shutdown();
        }
    }

    private void executeTheRowsJob(Session targetSession, ExecutorService executorService, List<String> primaryKeys, BoundStatement matchingBoundStatement, List<Row> rows) throws InterruptedException, java.util.concurrent.ExecutionException {
        Callable<Void> callable = new RowsValidatorJob(targetSession, primaryKeys, matchingBoundStatement, rows);
        Future<Void> future = executorService.submit(callable);
        future.get();
    }
}
