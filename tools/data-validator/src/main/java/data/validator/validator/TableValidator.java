package data.validator.validator;

import data.validator.DataValidator;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import org.apache.log4j.Logger;

/**
 * Created by Tarek on 5/2/2017.
 */
public class TableValidator implements Validator {
    final static Logger LOGGER = Logger.getLogger(TableValidator.class);
    @Override
    public void validate(CassandraConnectionProvider sourceCassandraConnectionProvider, CassandraConnectionProvider targetCassandraConnectionProvider, String sourceTableName, String targetTableName, int threadsCount) {
        LOGGER.info("Checking data integrity between source table {} and target table {}" , sourceTableName , targetTableName);
        DataValidator dataValidator = new DataValidator(sourceCassandraConnectionProvider, targetCassandraConnectionProvider);
        dataValidator.validate(sourceTableName, targetTableName, threadsCount);
    }
}
