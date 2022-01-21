package eu.europeana.cloud.service.dps.storm;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;

import java.util.List;

/**
 * Component responsible for executing provided statements in LOGGED batch
 */
public class BatchExecutor {

    private final CassandraConnectionProvider dbService;

    public BatchExecutor(CassandraConnectionProvider dbService) {
        this.dbService = dbService;
    }

    public void executeAll(List<BoundStatement> statements) {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.LOGGED);
        statements.forEach(batchStatement::add);
        dbService.getSession().execute(batchStatement);
    }
}
