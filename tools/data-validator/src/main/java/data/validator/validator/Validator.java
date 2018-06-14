package data.validator.validator;

import eu.europeana.cloud.cassandra.CassandraConnectionProvider;

import java.util.concurrent.ExecutionException;

/**
 * Created by Tarek on 5/2/2017.
 */
public interface Validator {
    void validate(CassandraConnectionProvider sourceCassandraConnectionProvider, CassandraConnectionProvider targetCassandraConnectionProvider, String sourceTableName, String targetTableName, int threadsCount) throws InterruptedException, ExecutionException;
}
