package migrations.service.mcs.V19;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import eu.europeana.cloud.service.commons.utils.RetryInterruptedException;

import java.util.*;


public class V19_1__fix_invalid_data implements JavaMigration {
    private static final Log LOG = LogFactory.getLog(V19_1__fix_invalid_data.class);

    private static final int DEFAULT_RETRIES = 3;
    private static final int RETRY_SLEEP_TIME = 5000;
    private static final int FETCH_SIZE = 1000;

    /** The name of metis provider. If this provider will be found in dataset_id column it is necessary to fix this item */
    private static final String METIS_PROVIDER = "metis_production";
    private static final String METIS_TEST_PROVIDER = "metis_acceptance";

    private PreparedStatement datasetSelectValues;
    private PreparedStatement datasetDeleteRow;
    private PreparedStatement datasetInsertRow;

    private void initStatements(Session session) {
        datasetSelectValues = session.prepare(
                "select provider_id, dataset_id, bucket_id, cloud_id, representation_id, revision_name, " +
                        "revision_provider, revision_timestamp, version_id, acceptance, published, mark_deleted " +
                        "from latest_dataset_representation_revision_v1;");

        datasetDeleteRow = session.prepare(
                "delete from latest_dataset_representation_revision_v1 " +
                        "where provider_id = ? and dataset_id = ? and bucket_id = ?;");

        datasetInsertRow = session.prepare(
                "insert into latest_dataset_representation_revision_v1 " +
                        "(provider_id, dataset_id,bucket_id,cloud_id,representation_id, revision_name, " +
                        "revision_provider, revision_timestamp,version_id, acceptance, published, mark_deleted) " +
                        "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);");
    }

    @Override
    public void migrate(Session session) {
        initStatements(session);

        //read all values
        BoundStatement datasetValuesStatement = datasetSelectValues.bind();
        datasetValuesStatement.setFetchSize(FETCH_SIZE);
        ResultSet resultSet = session.execute(datasetValuesStatement);
        Iterator<Row> iterator = resultSet.iterator();

        long totalCounter = 0;
        long fixedCounter = 0;

        boolean hasNext = iterator.hasNext();
            while (hasNext) {
                totalCounter++;

                if (totalCounter % 100000 == 0) {
                    LOG.info("V19_1__fix_invalid_data is working; totalCounter = " + totalCounter);
                }

                Row row = iterator.next();

                String providerId = row.getString("provider_id");
                String datasetId = row.getString("dataset_id");
                UUID bucketId = row.getUUID("bucket_id");

                if (isRatherProviderId(providerId, datasetId)) {
                    LOG.debug("Fixing item " + formatItemId(providerId, datasetId, bucketId));
                    swapValues(session, row, providerId, datasetId, bucketId);
                    fixedCounter++;
                }

                int attempt = 3;
                while(true) {
                    try {
                        if(attempt < 1) {
                            hasNext = false;
                            break;
                        }
                        attempt--;
                        hasNext = iterator.hasNext();

                        if(hasNext) {
                            break;
                        }
                    } catch (ReadTimeoutException rte) {
                        LOG.info("totalCounter = "+totalCounter + "; fixedCounter = "+fixedCounter);
                        LOG.warn(rte.getMessage());
                        try {
                            LOG.info("Waiting 10s ...");
                            Thread.sleep(10*1000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RetryInterruptedException(e);
                        }
                    }
                }
            }

        LOG.info("Fixing 'provider_id' <=> 'dataset_id' in 'latest_dataset_representation_revision_v1' finished." +
                " Total number of processed items: "+totalCounter+". Fixed items: "+fixedCounter+".");
    }

    private boolean isRatherProviderId(String providerId, String datasetId) {
        return (datasetId.equals(METIS_PROVIDER) && !providerId.equals(METIS_PROVIDER)) ||
                (datasetId.equals(METIS_TEST_PROVIDER) && !providerId.equals(METIS_TEST_PROVIDER));
    }

    private void swapValues(Session session, Row sourceRow, String providerId, String datasetId, UUID bucketId) {
        insertRow(session, sourceRow, providerId, datasetId, bucketId);
        deleteRow(session, providerId, datasetId, bucketId);
    }

    private void insertRow(Session session, Row sourceRow, String providerId, String datasetId, UUID bucketId) {
        BoundStatement datasetInsertRowStatement = datasetInsertRow.bind(
                /* Here dataset_id and provider_id are swapped */
                datasetId,
                providerId,

                /* Copy rest of fields */
                bucketId,
                sourceRow.getString("cloud_id"),
                sourceRow.getString("representation_id"),
                sourceRow.getString("revision_name"),
                sourceRow.getString("revision_provider"),
                sourceRow.getDate("revision_timestamp"),
                sourceRow.getUUID("version_id"),
                sourceRow.getBool("acceptance"),
                sourceRow.getBool("published"),
                sourceRow.getBool("mark_deleted")
        );

        executeStatementWithRetires(session, datasetInsertRowStatement,
                "inserting fixed data to 'latest_dataset_representation_revision_v1' table",
                formatItemId(providerId, datasetId, bucketId) + " in source row");
    }


    private void deleteRow(Session session, String providerId, String datasetId, UUID bucketId) {
        BoundStatement datasetDeleteRowStatement = datasetDeleteRow.bind(providerId, datasetId, bucketId);
        executeStatementWithRetires(session, datasetDeleteRowStatement,
                "deleting wrong data from 'latest_dataset_representation_revision_v1' table",
                formatItemId(providerId, datasetId, bucketId) + " in source row");
    }

    private void executeStatementWithRetires(Session session, BoundStatement statement, String message, String itemKey) {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                LOG.debug("Execute statement..."+statement.preparedStatement().getQueryString());
                session.execute(statement);
                break;
            } catch (Exception e) {
                if (retries-- > 0) {
                    LOG.warn("Warning while "+message+". Retries left:" + retries);
                    try {
                        Thread.sleep(RETRY_SLEEP_TIME);
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                        LOG.warn(e1.getMessage());
                    }
                } else {
                    LOG.error("Error while "+message+". Item: " + itemKey);
                    throw e;
                }
            }
        }
    }

    private String formatItemId(String providerId, String datasetId, UUID bucketId) {
        return String.format("(provider_id = %s, dataset_id = %s, bucket_id = %s)",
                providerId, datasetId, bucketId.toString());
    }

}

