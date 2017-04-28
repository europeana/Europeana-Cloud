package data.validator;

import com.datastax.driver.core.*;

import static data.validator.constants.Constants.*;

import data.validator.cql.CQLBuilder;
import data.validator.jobs.RowValidatorJob;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


/**
 * Created by Tarek on 4/26/2017.
 */


public class DataValidator {
    @Resource
    private CassandraConnectionProvider sourceCassandraConnectionProvider;

    @Resource
    private CassandraConnectionProvider targetCassandraConnectionProvider;


    private static final String SELECT_COLUMN_NAMES = "SELECT " + COLUMN_NAME_SELECTOR + ", " + COLUMN_INDEX_TYPE + " FROM " + SYSTEM_SCHEMA_COLUMNS_TABLE +
            "  WHERE keyspace_name =  ?  AND columnfamily_name = ? ;";


    public void validate(String sourceTableName, String targetTableName, int threadsCount) {
        Session session = null;
        Session targetSession = null;
        ExecutorService executorService = null;
        try {
            executorService = Executors.newFixedThreadPool(threadsCount);
            session = sourceCassandraConnectionProvider.getSession();
            List<String> primaryKeys = getPrimaryKeysNames(sourceTableName, session);
            String selectPrimaryKeysFromSourceTable = CQLBuilder.constructSelectPrimaryKeysFromSourceTable(sourceTableName, primaryKeys);
            PreparedStatement sourceSelectStatement = session.prepare(selectPrimaryKeysFromSourceTable);
            sourceSelectStatement.setConsistencyLevel(sourceCassandraConnectionProvider.getConsistencyLevel());
            BoundStatement boundStatement = sourceSelectStatement.bind();
            ResultSet rs = session.execute(boundStatement);
            Iterator<Row> iterator = rs.iterator();

            String matchCountStatementCQL = CQLBuilder.getMatchCountStatementFromTargetTable(targetTableName, primaryKeys);
            targetSession = targetCassandraConnectionProvider.getSession();
            PreparedStatement matchCountStatementCQLStatement = targetSession.prepare(matchCountStatementCQL);
            matchCountStatementCQLStatement.setConsistencyLevel(targetCassandraConnectionProvider.getConsistencyLevel());
            BoundStatement matchingBoundStatement = matchCountStatementCQLStatement.bind();

            while (iterator.hasNext()) {
                Row row = iterator.next();
                Callable<Long> callable = new RowValidatorJob(targetSession, primaryKeys, matchingBoundStatement, row);
                Future<Long> future = executorService.submit(callable);
                System.out.println("Thread tarek " + future.get());
            }

        } catch (Exception e) {
            System.out.println(e.getCause());
        } finally {
            if (session != null)
                session.close();
            if (targetSession != null)
                targetSession.close();
            sourceCassandraConnectionProvider.closeConnections();
            targetCassandraConnectionProvider.closeConnections();
            if (executorService!=null)
            executorService.shutdown();
        }

    }

    private List<String> getPrimaryKeysNames(String tableName, Session session) {
        List<String> names = new LinkedList<>();
        PreparedStatement selectStatement = session.prepare(SELECT_COLUMN_NAMES);
        selectStatement.setConsistencyLevel(sourceCassandraConnectionProvider.getConsistencyLevel());
        BoundStatement boundStatement = selectStatement.bind(sourceCassandraConnectionProvider.getKeyspaceName(), tableName);
        ResultSet rs = session.execute(boundStatement);
        Iterator<Row> iterator = rs.iterator();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            if (row.getString(COLUMN_INDEX_TYPE).equals(CLUSTERING_KEY_TYPE) || row.getString(COLUMN_INDEX_TYPE).equals(PARTITION_KEY_TYPE))
                names.add(row.getString(COLUMN_NAME_SELECTOR));
        }
        return names;
    }


}
