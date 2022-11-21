package migrations.service.mcs.V14;

import com.contrastsecurity.cassandra.migration.api.JavaMigration;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.datastax.driver.core.*;

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static migrations.common.TableCopier.hasNextRow;

public class V14_2__validate_data implements JavaMigration {

  private static final Log LOG = LogFactory.getLog(V14_2__validate_data.class);


  private PreparedStatement distinctPartitionValues;

  private ExecutorService executorService = Executors.newFixedThreadPool(10);

  private void initStatements(Session session) {
    distinctPartitionValues = session.prepare(
        "select DISTINCT provider_id, dataset_id from data_set_assignments_by_revision_id;");
  }

  @Override
  public void migrate(Session session) throws Exception {
    initStatements(session);
    BoundStatement partitionValuesStatement = distinctPartitionValues.bind();

    partitionValuesStatement.setFetchSize(1000);
    ResultSet resultSet = session.execute(partitionValuesStatement);
    Iterator<Row> iterator = resultSet.iterator();

    while (hasNextRow(iterator)) {
      Row row = iterator.next();
      LOG.info("Submitting task for: " + row.getString("provider_id") + ":" + row.getString("dataset_id"));
      executorService.submit(new ValidationJob(session, row.getString("provider_id"), row.getString("dataset_id")));
    }
    executorService.shutdown();
    executorService.awaitTermination(100, TimeUnit.DAYS);
    //
  }
}

class ValidationJob implements Runnable {

  private static final Log LOG = LogFactory.getLog(ValidationJob.class);

  protected static final String CDSID_SEPARATOR = "\n";


  private static final int DEFAULT_RETRIES = 3;
  private static final int SLEEP_TIME = 5000;

  private Session session;
  private String provider_id;
  private String dataset_id;

  private PreparedStatement selectStatement;

  private PreparedStatement bucketsForSpecificObjectIdStatement;

  private PreparedStatement countOfRowsStatementFromV1;
  private PreparedStatement countOfRowsStatementFromReplica;


  private void initStatements(Session session) {
    selectStatement = session.prepare(
        "SELECT * FROM data_set_assignments_by_revision_id where provider_id = ? and dataset_id = ?");

    bucketsForSpecificObjectIdStatement = session.prepare(
        "Select bucket_id from data_set_assignments_by_revision_id_buckets where object_id=?");

    countOfRowsStatementFromV1 = session.prepare(
        "Select count(*) from data_set_assignments_by_revision_id_v1 where provider_id=? and dataset_id=? and bucket_id=? and revision_provider_id=? and revision_name=? and revision_timestamp=? and representation_id=? and cloud_id=? ;");
    countOfRowsStatementFromReplica = session.prepare(
        "Select count(*) from data_set_assignments_by_revision_id_replica where provider_id=? and dataset_id=? and revision_provider_id=? and revision_name=? and revision_timestamp=? and representation_id=? and cloud_id=? ;");

    selectStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
    countOfRowsStatementFromV1.setConsistencyLevel(ConsistencyLevel.QUORUM);
    countOfRowsStatementFromReplica.setConsistencyLevel(ConsistencyLevel.QUORUM);
    bucketsForSpecificObjectIdStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);
  }

  public ValidationJob(Session session, String provider_id, String dataset_id) {
    this.session = session;
    this.provider_id = provider_id;
    this.dataset_id = dataset_id;
  }

  @Override
  public void run() {
    try {
      LOG.info("Starting validation for for: " + provider_id + ":" + dataset_id);
      initStatements(session);

      long counter = 0;

      BoundStatement boundStatement = selectStatement.bind(provider_id, dataset_id);
      boundStatement.setFetchSize(1000);
      ResultSet rs = session.execute(boundStatement);

      Iterator<Row> ri = rs.iterator();

      while (hasNextRow(ri)) {
        Row dataset_assignment = ri.next();
        matchRecord(session, dataset_assignment);

        if (++counter % 10000 == 0) {
          LOG.info("validating table progress for: " + provider_id + ":" + dataset_id + "id =" + counter);
        }
      }
      LOG.info("Validating finished successfully for: " + provider_id + ":" + dataset_id + ". Validated rows: " + counter);
    } catch (Exception e) {
      LOG.info("FAILED to execute Validation for: " + provider_id + ":" + dataset_id + "." + e.getMessage()
          + " . The cause of the problem: " + e.getCause());
    }

  }

  private String createProviderDataSetId(String providerId, String dataSetId) {
    return providerId + CDSID_SEPARATOR + dataSetId;
  }


  private void matchRecord(Session session, Row dataset_assignment) {

    String objectId = createProviderDataSetId(dataset_assignment.getString("provider_id"),
        dataset_assignment.getString("dataset_id"));
    BoundStatement boundStatement = bucketsForSpecificObjectIdStatement.bind(objectId);
    ResultSet rs = session.execute(boundStatement);
    Iterator<Row> ri = rs.iterator();

    while (hasNextRow(ri)) {
      UUID bucketId = ri.next().getUUID("bucket_id");
      long count = getMatchCount(dataset_assignment, bucketId, countOfRowsStatementFromV1);
      if (count > 0) {
        return;
      }

    }
    long count = getMatchCountFromReplica(dataset_assignment, countOfRowsStatementFromReplica);
    if (count > 0) {
      return;
    }

    throw new RuntimeException("The record with providerId= " + dataset_assignment.getString("provider_id") + " & datasetId="
        + dataset_assignment.getString("dataset_id") +
        " & revision_name=" + dataset_assignment.getString("revision_name") + " & cloud_id=" + dataset_assignment.getString(
        "cloud_id") + " couldn't be validated");
  }


  private long getMatchCount(Row dataset_assignment, UUID bucketId, PreparedStatement countOfRowsStatement) {
    BoundStatement countBoundStatement = countOfRowsStatement.bind(dataset_assignment.getString("provider_id"),
        dataset_assignment.getString("dataset_id"),
        bucketId,
        dataset_assignment.getString("revision_provider_id"),
        dataset_assignment.getString("revision_name"),
        dataset_assignment.getDate("revision_timestamp"),
        dataset_assignment.getString("representation_id"),
        dataset_assignment.getString("cloud_id")
    );

    int retries = DEFAULT_RETRIES;

    while (true) {
      try {
        ResultSet resultSet = session.execute(countBoundStatement);
        return resultSet.one().getLong(0);
      } catch (Exception e) {
        if (retries-- > 0) {
          LOG.info("Warning while matching record to data_set_assignments_by_revision_id. Retries left:" + retries);
          try {
            Thread.sleep(SLEEP_TIME);
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            System.err.println(e1.getMessage());
          }
        } else {
          LOG.error(
              "Error while matching record for data_set_assignments_by_revision_id. " + countBoundStatement.preparedStatement()
                                                                                                           .getQueryString());
          throw e;
        }
      }
    }
  }


  private long getMatchCountFromReplica(Row dataset_assignment, PreparedStatement countOfRowsStatement) {
    BoundStatement countBoundStatement = countOfRowsStatement.bind(dataset_assignment.getString("provider_id"),
        dataset_assignment.getString("dataset_id"),
        dataset_assignment.getString("revision_provider_id"),
        dataset_assignment.getString("revision_name"),
        dataset_assignment.getDate("revision_timestamp"),
        dataset_assignment.getString("representation_id"),
        dataset_assignment.getString("cloud_id")
    );

    int retries = DEFAULT_RETRIES;

    while (true) {
      try {
        ResultSet resultSet = session.execute(countBoundStatement);
        return resultSet.one().getLong(0);
      } catch (Exception e) {
        if (retries-- > 0) {
          LOG.info("Warning while matching record from data_set_assignments_by_revision_id to replica. Retries left:" + retries);
          try {
            Thread.sleep(SLEEP_TIME);
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            System.err.println(e1.getMessage());
          }
        } else {
          LOG.error("Error while matching record drom data_set_assignments_by_revision_id to replica. "
              + countBoundStatement.preparedStatement().getQueryString());
          throw e;
        }
      }
    }
  }
}


