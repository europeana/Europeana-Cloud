package migrations.service.mcs.V19;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.datastax.driver.core.*;

import java.util.*;


public class V19_1__fix_invalid_data implements JavaMigration {
    private static final Log LOG = LogFactory.getLog(V19_1__fix_invalid_data.class);

    private static final int DEFAULT_RETRIES = 3;
    private static final int RETRY_SLEEP_TIME = 5000;
    private static final int FETCH_SIZE = 1000;

    /** Current (12.09.2019) set of providers. If this migration will be called in far future, this list should be updated (maybe) */
    private static final Set<String> AVAILABLE_PROVIDERS;
    static {
        AVAILABLE_PROVIDERS = new HashSet<>(Arrays.asList(
                //Provaiders in the production database
                "AveroffMuseum",
                "bakkerijmuseum",
                "Boutari",
                "CAG",
                "COnnectingREpositories",
                "DianthaOs",
                "eCyclades",
                "edoao",
                "Europeana_Foundation",
                "GreekWineCellars",
                "hallwylskamuseet",
                "Lazaridi",
                "livrustkammaren",
                "metis_production",
                "MyEuropeana user abc1234",
                "Mylonas",
                "nagios-health-check",
                "Nationalmuseum_Sweden",
                "ola-test",
                "p2",
                "Papagianakos",
                "Provider___ 2017_05_26_16_02_16_1_1",
                "Provider___ 2017_08_02_08_43_12_1_2",
                "Provider___ 2017_08_02_08_43_12_2_2",
                "Provider___ 2017_08_02_08_43_29_1_1",
                "Provider___ 2017_08_02_08_43_29_1_2",
                "Provider___ 2017_08_02_08_43_29_2_1",
                "Provider___ 2017_08_02_08_43_29_2_2",
                "Provider___ 2017_12_14_08_33_35_1_1",
                "Provider___ 2017_12_14_08_33_58_1_1",
                "Provider___ 2017_12_14_08_34_14_1_1",
                "Provider___ 2017_12_14_08_37_21_1_1",
                "Provider___ 2017_12_14_08_37_39_1_1",
                "Provider___ 2017_12_14_08_37_55_1_1",
                "Provider___ 2017_12_14_08_39_17_1_1",
                "Provider___ 2017_12_14_08_39_34_1_1",
                "Provider___ 2017_12_14_08_39_57_1_1",
                "Provider___ 2017_12_14_08_45_25_1_1",
                "Provider___ 2017_12_14_08_46_25_1_1",
                "Provider___ 2017_12_14_08_46_40_1_1",
                "Provider___ 2017_12_14_08_46_50_1_1",
                "Provider___ 2017_12_14_08_48_16_1_1",
                "Provider___ 2017_12_14_08_48_28_1_1",
                "Provider___ 2017_12_14_08_48_38_1_1",
                "richard.doe@europeana.eu",
                "rmca",
                "skoklostersslott",
                "Strofilia_1",
                "The_European_Library",
                "TheEuropeanLibrary",
                "Vasileiou",
                "VUALib",
                //Provaiders in the test database
                "prov",
                "metis_acceptance",
                "metis_test5",
                "Arek-TEST-02",
                "lalal_provider",
                "Arek_provider",
                "Arek-01-Test",
                "prov2",
                "ola-test1")
        );
    }

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

        int totalCounter = 0;
        int fixedCounter = 0;

        while(iterator.hasNext()) {
            totalCounter++;

            Row row = iterator.next();

            String providerId = row.getString("provider_id");
            String datasetId = row.getString("dataset_id");
            UUID bucketId = row.getUUID("bucket_id");

            if(isRatherProviderId(datasetId)) {
                LOG.debug("Fixing item "+formatItemId(providerId, datasetId, bucketId));
                swapValues(session, row, providerId, datasetId, bucketId);
                fixedCounter++;
            }
        }
        LOG.info("Fixing 'provider_id' <=> 'dataset_id' in 'latest_dataset_representation_revision_v1' ended." +
                " Total number of processed items: "+totalCounter+". Fixed items: "+fixedCounter+".");
    }

    private boolean isRatherProviderId(String datasetId) {
        return AVAILABLE_PROVIDERS.contains(datasetId);
    }

    private void swapValues(Session session, Row sourceRow, String providerId, String datasetId, UUID bucketId) {
        insertRow(session, sourceRow, providerId, datasetId, bucketId);
        deleteRow(session, providerId, datasetId, bucketId);
    }

    private void insertRow(Session session, Row sourceRow, String providerId, String datasetId, UUID bucketId) {
        BoundStatement datasetInsertRowStatement = datasetInsertRow.bind(
                /* Here dataset_id and provider_id are swaped */
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

