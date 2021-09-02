package data.validator.validator;

import data.validator.DataValidator;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Tarek on 5/2/2017.
 */
public class TableValidator implements Validator {
    final static Logger LOGGER = LoggerFactory.getLogger(TableValidator.class);
    @Override
    public void validate(CassandraConnectionProvider sourceCassandraConnectionProvider, CassandraConnectionProvider targetCassandraConnectionProvider, String sourceTableName, String targetTableName, int threadsCount) {
        LOGGER.info("Checking data integrity between source table "+sourceTableName+" and target table "+targetTableName);
        DataValidator dataValidator = new DataValidator(sourceCassandraConnectionProvider, targetCassandraConnectionProvider);
        dataValidator.validate(sourceTableName, targetTableName, threadsCount);
    }
}
