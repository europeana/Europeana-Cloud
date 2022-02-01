package eu.europeana.cloud.service.dps.storm;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;

import java.util.Arrays;

/**
 * Component responsible for executing provided statements in LOGGED batch
 */
public class BatchExecutor {

    private final CassandraConnectionProvider dbService;

    private static BatchExecutor instance = null;

    public BatchExecutor(CassandraConnectionProvider dbService) {
        this.dbService = dbService;
    }

    public static synchronized BatchExecutor getInstance(CassandraConnectionProvider cassandra) {
        if (instance == null) {
            instance = new BatchExecutor(cassandra);
        }
        return instance;
    }

    public void executeAll(BoundStatement... statements) {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.LOGGED);
        Arrays.stream(statements).forEach(batchStatement::add);
        dbService.getSession().execute(batchStatement);
    }
}
