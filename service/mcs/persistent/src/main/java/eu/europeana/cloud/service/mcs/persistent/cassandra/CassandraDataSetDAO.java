package eu.europeana.cloud.service.mcs.persistent.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.utils.Bucket;
import eu.europeana.cloud.service.mcs.persistent.util.QueryTracer;
import jakarta.annotation.PostConstruct;

import java.util.*;

import static eu.europeana.cloud.service.mcs.persistent.cassandra.PersistenceUtils.createProviderDataSetId;

/**
 * Data set repository that uses Cassandra nosql database.
 */
@Retryable
public class CassandraDataSetDAO {

  // separator between provider id and dataset id in serialized compound
  // dataset id
  public static final String CDSID_SEPARATOR = "\n";

  public static final int MAX_DATASET_ASSIGNMENTS_BUCKET_COUNT = 100_000;

  public static final String DATA_SET_ASSIGNMENTS_BY_DATA_SET_BUCKETS = "data_set_assignments_by_data_set_buckets";

  private final CassandraConnectionProvider connectionProvider;
  private PreparedStatement createDataSetStatement;
  private PreparedStatement deleteDataSetStatement;
  private PreparedStatement addAssignmentStatement;
  private PreparedStatement removeAssignmentStatement;
  private PreparedStatement listDataSetRepresentationsStatement;
  private PreparedStatement listExistingDataSetRepresentationsStatement;
  private PreparedStatement listDataSetsStatement;
  private PreparedStatement getDataSetStatement;

  /**
   * Constructor for the class
   *
   * @param connectionProvider connection provider for DB
   */
  public CassandraDataSetDAO(CassandraConnectionProvider connectionProvider) {
    this.connectionProvider = connectionProvider;
  }

  /**
   * Reads assignments for the given dataset;
   *
   * @param providerDataSetId concatenation of providerId and datasetId
   * @param bucketId bucket identifier
   * @param state paging state
   * @param limit limit of results
   *
   * @return slice of the results
   */
  public ResultSlice<DatasetAssignment> getDataSetAssignments(String providerDataSetId, String bucketId, PagingState state,
      int limit, boolean existingOnly) {
    List<DatasetAssignment> assignments = new ArrayList<>();
    // bind parameters, set limit to max int value
    BoundStatement boundStatement;
    if (existingOnly) {
      boundStatement = listExistingDataSetRepresentationsStatement.bind(
              providerDataSetId, UUID.fromString(bucketId), Integer.MAX_VALUE);
    } else {
      boundStatement = listDataSetRepresentationsStatement.bind(
              providerDataSetId, UUID.fromString(bucketId), Integer.MAX_VALUE);
    }
    // limit page to "limit" number of results
    boundStatement.setFetchSize(limit);
    // when this is not a first page call set paging state in the statement
    if (state != null) {
      boundStatement.setPagingState(state);
    }

    // execute query
    ResultSet rs = connectionProvider.getSession().execute(boundStatement);
    QueryTracer.logConsistencyLevel(boundStatement, rs);

    // get available results
    int available = rs.getAvailableWithoutFetching();
    for (int i = 0; i < available; i++) {
      Row row = rs.one();
      assignments.add(DatasetAssignment.from(row));
    }
    PagingState ps = rs.getExecutionInfo().getPagingState();

    if (assignments.size() == limit && !rs.isExhausted()) {
      return new ResultSlice<>(Optional.ofNullable(ps)
                                       .map(Object::toString).orElse(null), assignments);
    } else {
      return new ResultSlice<>(null, assignments);
    }

  }


  /**
   * Inserts new row to the <b><i>data_set_assignments_by_data_set</i></b> table.
   *
   * @param providerId dataset provider
   * @param dataSetId dataset name
   * @param bucketId  bucket identifier
   * @param recordId  cloud identifier
   * @param schema representation name
   * @param now time of assignment
   * @param versionId representation version
   */
  public void addAssignment(String providerId, String dataSetId, String bucketId, String recordId, String schema, Date now,
                            UUID versionId, boolean markDepublished) {
    String providerDataSetId = createProviderDataSetId(providerId, dataSetId);
    BoundStatement boundStatement = addAssignmentStatement.bind(
            providerDataSetId, UUID.fromString(bucketId), schema, recordId, versionId, now, markDepublished);
    ResultSet rs = connectionProvider.getSession().execute(boundStatement);
    QueryTracer.logConsistencyLevel(boundStatement, rs);
  }



  /**
   * Returns data set from specified provider with specified id. Throws exception when provider does not exist. Returns null if
   * provider exists but does not have data set with specified id.
   *
   * @param providerId data set owner's (provider's) id
   * @param dataSetId data set id
   * @return data set
   *
   * @throws NoHostAvailableException in case of Cassandra issues
   * @throws QueryExecutionException in case of Cassandra issues
   */
  public DataSet getDataSet(String providerId, String dataSetId) throws NoHostAvailableException, QueryExecutionException {

    BoundStatement boundStatement = getDataSetStatement.bind(providerId, dataSetId);
    ResultSet rs = connectionProvider.getSession().execute(boundStatement);
    QueryTracer.logConsistencyLevel(boundStatement, rs);
    Row row = rs.one();
    if (row == null) {
      return null;
    }
    DataSet ds = new DataSet();
    ds.setProviderId(providerId);
    ds.setId(dataSetId);
    ds.setDescription(row.getString("description"));
    return ds;
  }

  /**
   * Deletes row from <b><i>data_set_assignments_by_data_set</i></b> table
   *
   * @param recordId  cloud identifier
   * @param schema representation name
   * @param versionId representation version
   * @param providerDataSetId concatenated provider and datasetId
   * @param bucket bucket identifier
   * @return if operation was applied
   *
   */
  public boolean removeDatasetAssignment(String recordId, String schema, String versionId, String providerDataSetId,
      Bucket bucket) {
    BoundStatement boundStatement = removeAssignmentStatement.bind(
        providerDataSetId, UUID.fromString(bucket.getBucketId()), schema, recordId, UUID.fromString(versionId));
    ResultSet rs = connectionProvider.getSession().execute(boundStatement);
    QueryTracer.logConsistencyLevel(boundStatement, rs);
    return rs.wasApplied();
  }


  /**
   * Creates or updates data set for a provider.
   *
   * @param providerId data set owner's (provider's) id
   * @param dataSetId data set id
   * @param description description of data set.
   * @param creationTime creation date
   * @return created (or updated) data set.
   *
   * @throws NoHostAvailableException in case of Cassandra issues
   * @throws QueryExecutionException in case of Cassandra issues
   */
  public DataSet createDataSet(String providerId, String dataSetId, String description, Date creationTime)
      throws NoHostAvailableException, QueryExecutionException {

    BoundStatement boundStatement = createDataSetStatement.bind(providerId, dataSetId, description, creationTime);
    ResultSet rs = connectionProvider.getSession().execute(boundStatement);
    QueryTracer.logConsistencyLevel(boundStatement, rs);
    DataSet ds = new DataSet();
    ds.setId(dataSetId);
    ds.setDescription(description);
    ds.setProviderId(providerId);
    return ds;
  }

  /**
   * Lists all data sets for a provider.
   *
   * @param providerId data set owner's (provider's) id
   * @param thresholdDatasetId parameter used to pagination, returned representations wil have dataSetId >= thresholdDatasetId.
   * Might be null.
   * @param limit max size of returned data set list.
   * @return list of data sets.
   *
   * @throws NoHostAvailableException in case of Cassandra issues
   * @throws QueryExecutionException in case of Cassandra issues
   */
  public List<DataSet> getDataSets(String providerId, String thresholdDatasetId, int limit)
      throws NoHostAvailableException, QueryExecutionException {

    BoundStatement boundStatement = listDataSetsStatement.bind(
        providerId, thresholdDatasetId != null ? thresholdDatasetId : "", limit);

    ResultSet rs = connectionProvider.getSession().execute(boundStatement);
    QueryTracer.logConsistencyLevel(boundStatement, rs);

    List<DataSet> result = new ArrayList<>(limit);
    for (Row row : rs) {
      DataSet ds = new DataSet();
      ds.setProviderId(providerId);
      ds.setId(row.getString("dataset_id"));
      ds.setDescription(row.getString("description"));
      result.add(ds);
    }

    return result;
  }

  /**
   * Deletes data set
   *
   * @param providerId data set owner's (provider's) id
   * @param dataSetId data set id
   *
   * @throws NoHostAvailableException in case of Cassandra issues
   * @throws QueryExecutionException in case of Cassandra issues
   */
  public void deleteDataSet(String providerId, String dataSetId) throws NoHostAvailableException, QueryExecutionException {
    BoundStatement boundStatement = deleteDataSetStatement.bind(providerId, dataSetId);
    connectionProvider.getSession().execute(boundStatement);
  }

    //  Need separate function so mock in test can modify it
    @PostConstruct
    private void postConstruct() {
        prepareStatements();
    }

    void prepareStatements() {
        createDataSetStatement = connectionProvider.getSession().prepare(
                "INSERT INTO " +
                        "data_sets(provider_id, dataset_id, description, creation_date) " +
                        "VALUES (?,?,?,?);"
        );

        deleteDataSetStatement = connectionProvider.getSession().prepare(
                "DELETE FROM " +
                        "data_sets " +
                        "WHERE provider_id = ? AND dataset_id = ?;"
    );

    addAssignmentStatement = connectionProvider.getSession().prepare(
        "INSERT " +
            "INTO data_set_assignments_by_data_set (provider_dataset_id, bucket_id, schema_id, cloud_id, version_id, creation_date, mark_deleted) "
            +
            "VALUES (?,?,?,?,?,?,?);"
    );


    removeAssignmentStatement = connectionProvider.getSession().prepare(
        "DELETE " +
            "FROM data_set_assignments_by_data_set " +
            "WHERE provider_dataset_id = ? AND bucket_id = ? AND schema_id = ? AND cloud_id = ? AND version_id = ? IF EXISTS;"
    );


    listDataSetRepresentationsStatement = connectionProvider.getSession().prepare(
        "SELECT cloud_id, schema_id, version_id " +
            "FROM data_set_assignments_by_data_set " +
            "WHERE provider_dataset_id = ? AND bucket_id = ? " +
            "LIMIT ?;"
    );

    listExistingDataSetRepresentationsStatement = connectionProvider.getSession().prepare(
            "SELECT cloud_id, schema_id, version_id " +
                    "FROM data_set_assignments_by_data_set " +
                    "WHERE provider_dataset_id = ? AND bucket_id = ? " +
                    "AND mark_deleted = false " +
                    "LIMIT ? ALLOW FILTERING;"
    );

    listDataSetsStatement = connectionProvider.getSession().prepare(
        "SELECT provider_id, dataset_id, description " +
            "FROM data_sets " +
            "WHERE provider_id = ? AND dataset_id >= ?" +
            "LIMIT ?;"
    );

    getDataSetStatement = connectionProvider.getSession().prepare(
        "SELECT provider_id, dataset_id, description " +
            "FROM data_sets " +
            "WHERE provider_id = ? AND dataset_id = ?;"
    );

  }

}
