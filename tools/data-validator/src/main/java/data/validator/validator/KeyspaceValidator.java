package data.validator.validator;

import com.datastax.driver.core.TableMetadata;
import data.validator.DataValidator;
import data.validator.jobs.TableValidatorJob;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Created by Tarek on 5/2/2017.
 */
public class KeyspaceValidator implements Validator {
    final static Logger LOGGER = Logger.getLogger(KeyspaceValidator.class);

    @Override
    public void validate(CassandraConnectionProvider sourceCassandraConnectionProvider, CassandraConnectionProvider targetCassandraConnectionProvider, String sourceTableName, String targetTableName, int threadsCount) throws InterruptedException, ExecutionException {
        ExecutorService executorService = null;
        try {
            Iterator<TableMetadata> tmIterator = sourceCassandraConnectionProvider.getMetadata().getKeyspace(sourceCassandraConnectionProvider.getKeyspaceName()).getTables().iterator();
            final Set<Callable<Void>> tableValidatorJobs = new HashSet<>();
            executorService = Executors.newFixedThreadPool(threadsCount);
            while (tmIterator.hasNext()) {
                CassandraConnectionProvider newSourceCassandraConnectionProvider = new CassandraConnectionProvider(sourceCassandraConnectionProvider);
                CassandraConnectionProvider newTargetCassandraConnectionProvider = new CassandraConnectionProvider(targetCassandraConnectionProvider);
                DataValidator dataValidator = new DataValidator(newSourceCassandraConnectionProvider, newTargetCassandraConnectionProvider);
                TableMetadata t = tmIterator.next();
                LOGGER.info("Checking data integrity between source table " + t.getName() + " and target table " + t.getName());
                tableValidatorJobs.add(new TableValidatorJob(dataValidator, t.getName(), t.getName(), threadsCount));
            }
            List<Future<Void>> results = executorService.invokeAll(tableValidatorJobs);
            for (Future<Void> future : results) {
                future.get();
            }
        } finally

        {
            if (executorService != null)
                executorService.shutdown();
            if (sourceCassandraConnectionProvider != null)
                sourceCassandraConnectionProvider.closeConnections();
            if (targetCassandraConnectionProvider != null)
                targetCassandraConnectionProvider.closeConnections();
        }
    }
}
