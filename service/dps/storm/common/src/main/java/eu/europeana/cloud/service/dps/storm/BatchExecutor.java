package eu.europeana.cloud.service.dps.storm;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;

import java.util.List;

/**
 * Component responsible for executing provided statements in LOGGED batch
 */
public class BatchExecutor {

    private static final int RETRY_COUNT = 10;
    private static final int SLEEP_BETWEEN_RETRIES_MS = 10000;

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

    public void executeAll(List<BoundStatement> statements) {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.LOGGED);
        statements.forEach(batchStatement::add);
        executeWithRetries(batchStatement);
    }

    private void executeWithRetries(BatchStatement batchStatement) {
        RetryableMethodExecutor.execute("Unable to execute batch", RETRY_COUNT,
                SLEEP_BETWEEN_RETRIES_MS, () -> {
                    dbService.getSession().execute(batchStatement);
                    return null;
                });
    }
}
