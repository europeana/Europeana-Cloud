package migrations.service.mcs.V18;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.datastax.driver.core.*;
import migrations.service.mcs.V18.jobs.DataCopier;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;

import static migrations.common.TableCopier.hasNextRow;

/**
 * Created by Tarek on 4/26/19
 */
public class V18_1__copy_data_from_replica implements JavaMigration {

    private static final Log LOG = LogFactory.getLog(V18_1__copy_data_from_replica.class);

    public static final int THREADS = 25;
    private PreparedStatement selectDistinctPartitionKeysStatement;

    private void initStatements(Session session) {
        selectDistinctPartitionKeysStatement = session.prepare("SELECT DISTINCT provider_id,dataset_id FROM latest_provider_dataset_rep_rev_replica");
        selectDistinctPartitionKeysStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
    }

    @Override
    public void migrate(Session session) {
        ExecutorService executorService = Executors.newFixedThreadPool(THREADS);

        try {
            initStatements(session);
            ResultSet distinctPartitions = session.execute(selectDistinctPartitionKeysStatement.bind());
            Iterator<Row> distinctPartitionIterator = distinctPartitions.iterator();
            while (hasNextRow(distinctPartitionIterator)) {
                Row disRow = distinctPartitionIterator.next();
                LOG.info("Submitting task for: " + disRow.getString("provider_id") + ":" + disRow.getString("dataset_id"));
                DataCopier dataCopier = new DataCopier(session, disRow.getString("provider_id"), disRow.getString("dataset_id"));
                executorService.submit(dataCopier);
            }
        } catch (Exception e) {
            LOG.error("The migration was not completed successfully:" + e.getMessage() + ". Because of:" + e.getCause() + " .Please clean and restart again!!");
        }

        try {
            executorService.shutdown();
            executorService.awaitTermination(100, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}