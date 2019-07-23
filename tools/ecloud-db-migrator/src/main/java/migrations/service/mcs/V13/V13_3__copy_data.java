package migrations.service.mcs.V13;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.datastax.driver.core.*;
import eu.europeana.cloud.common.utils.Bucket;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static migrations.common.TableCopier.hasNextRow;

public class V13_3__copy_data implements JavaMigration {

    private static final Log LOG = LogFactory.getLog(V13_3__copy_data.class);

    private PreparedStatement distinctPartitionValues;

    private ExecutorService executorService = Executors.newFixedThreadPool(10);

    private void initStatements(Session session) {
        distinctPartitionValues = session.prepare("select DISTINCT provider_id, dataset_id from data_set_assignments_by_revision_id;");
    }

    @Override
    public void migrate(Session session) throws Exception {

        initStatements(session);

        //read all distinct values
        BoundStatement partitionValuesStatement = distinctPartitionValues.bind();
        partitionValuesStatement.setFetchSize(1000);
        ResultSet resultSet = session.execute(partitionValuesStatement);
        Iterator<Row> iterator = resultSet.iterator();

        while (hasNextRow(iterator)) {
            Row row = iterator.next();
            LOG.info("Submitting task for: " + row.getString("provider_id") + ":" + row.getString("dataset_id"));
            executorService.submit(new PartitionMigrator(session, row.getString("provider_id"), row.getString("dataset_id")));
        }
        executorService.shutdown();
        executorService.awaitTermination(100, TimeUnit.DAYS);
        //
    }
}

class PartitionMigrator implements Runnable {

    private static final Log LOG = LogFactory.getLog(PartitionMigrator.class);

    protected static final String CDSID_SEPARATOR = "\n";
    private static final int MAX_DATASET_ASSIGNMENTS_BY_REVISION_ID_BUCKET_COUNT = 250000;

    private Session session;
    private String provider_id;
    private String dataset_id;

    private PreparedStatement selectStatement;

    private PreparedStatement insertStatement;

    private void initStatements(Session session) {
        selectStatement = session.prepare("SELECT * FROM data_set_assignments_by_revision_id where provider_id = ? and dataset_id = ?");

        insertStatement = session.prepare("INSERT INTO "
                + "data_set_assignments_by_revision_id_v1 (provider_id,dataset_id,bucket_id,revision_provider_id,revision_name,revision_timestamp,representation_id,cloud_id,published,acceptance, mark_deleted) "
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?);");

        selectStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
        insertStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
    }

    public PartitionMigrator(Session session, String provider_id, String dataset_id) {
        this.session = session;
        this.provider_id = provider_id;
        this.dataset_id = dataset_id;
    }

    @Override
    public void run() {
        try{
            LOG.info("Starting migration for for: " + provider_id + ":" + dataset_id);
            BucketsHandler bucketsHandler = new BucketsHandler(session);
            initStatements(session);

            long counter = 0;

            BoundStatement boundStatement = selectStatement.bind(provider_id, dataset_id);
            boundStatement.setFetchSize(1000);
            ResultSet rs = session.execute(boundStatement);

            Iterator<Row> ri = rs.iterator();

            while (hasNextRow(ri)) {
                Row dataset_assignment = ri.next();
                //
                Bucket bucket = bucketsHandler.getCurrentBucket(
                        "data_set_assignments_by_revision_id_buckets",
                        createProviderDataSetId(dataset_assignment.getString("provider_id"), dataset_assignment.getString("dataset_id")));

                if (bucket == null || bucket.getRowsCount() >= MAX_DATASET_ASSIGNMENTS_BY_REVISION_ID_BUCKET_COUNT) {
                    bucket = new Bucket(
                            createProviderDataSetId(dataset_assignment.getString("provider_id"), dataset_assignment.getString("dataset_id")),
                            new com.eaio.uuid.UUID().toString(),
                            0);
                }
                bucketsHandler.increaseBucketCount("data_set_assignments_by_revision_id_buckets", bucket);
                //
                insertRowToAssignmentsByRepresentationsTable(session, dataset_assignment, bucket.getBucketId());

                if (++counter % 10000 == 0) {
                    LOG.info("Copy table progress: " + counter);
                }
            }
            LOG.info("Migration finished successfully for: " + provider_id + ":" + dataset_id + ". Migrated rows: " + counter);
        }catch(Exception e){
            LOG.info("FAILED to execute migration for: " + provider_id + ":" + dataset_id);
        }

    }

    private void insertRowToAssignmentsByRepresentationsTable(Session session, Row sourceRow, String bucketId) {

        BoundStatement boundStatement = insertStatement.bind(
                sourceRow.getString("provider_id"),
                sourceRow.getString("dataset_id"),
                UUID.fromString(bucketId),
                sourceRow.getString("revision_provider_id"),
                sourceRow.getString("revision_name"),
                sourceRow.getDate("revision_timestamp"),
                sourceRow.getString("representation_id"),
                sourceRow.getString("cloud_id"),
                sourceRow.getBool("published"),
                sourceRow.getBool("acceptance"),
                sourceRow.getBool("mark_deleted")
        );
        session.execute(boundStatement);
    }

    private String createProviderDataSetId(String providerId, String dataSetId) {
        return providerId + CDSID_SEPARATOR + dataSetId;
    }
}
