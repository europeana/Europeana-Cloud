package eu.europeana.cloud.service.mcs.persistent.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.CloudTagsResponse;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.common.utils.Bucket;
import eu.europeana.cloud.service.commons.utils.BucketsHandler;
import eu.europeana.cloud.service.mcs.persistent.util.QueryTracer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.util.*;

import static eu.europeana.cloud.service.mcs.persistent.cassandra.PersistenceUtils.createCompoundDataSetId;
import static eu.europeana.cloud.service.mcs.persistent.cassandra.PersistenceUtils.createProviderDataSetId;

/**
 * Data set repository that uses Cassandra nosql database.
 */
@Repository
@Retryable
public class CassandraDataSetDAO {

    // separator between provider id and dataset id in serialized compund
    // dataset id
    public static final String CDSID_SEPARATOR = "\n";

    public static final int MAX_DATASET_ASSIGNMENTS_BUCKET_COUNT = 100000;
    public static final int MAX_DATASET_ASSIGNMENTS_BY_REVISION_ID_BUCKET_COUNT = 125000;

    public static final String DATA_SET_ASSIGNMENTS_BY_DATA_SET_BUCKETS = "data_set_assignments_by_data_set_buckets";

    public static final String DATA_SET_ASSIGNMENTS_BY_REVISION_ID_BUCKETS = "data_set_assignments_by_revision_id_buckets";

    private static final String PROVIDER_DATASET_ID = "provider_dataset_id";

    @Autowired
    @Qualifier("dbService")
    private CassandraConnectionProvider connectionProvider;

    @Autowired
    private BucketsHandler bucketsHandler;

    private PreparedStatement createDataSetStatement;

    private PreparedStatement deleteDataSetStatement;

    private PreparedStatement addAssignmentStatement;

    private PreparedStatement addAssignmentByRepresentationStatement;

    private PreparedStatement removeAssignmentByRepresentationsStatement;

    private PreparedStatement removeAssignmentStatement;

    private PreparedStatement listDataSetRepresentationsStatement;

    private PreparedStatement listDataSetsStatement;

    private PreparedStatement getDataSetStatement;

    private PreparedStatement getDataSetsForRepresentationVersionStatement;

    private PreparedStatement getOneDataSetForRepresentationStatement;

    private PreparedStatement getDataSetsRepresentationsNamesListStatement;

    private PreparedStatement addDataSetsRepresentationNameStatement;

    private PreparedStatement removeDataSetsRepresentationNameStatement;

    private PreparedStatement removeDataSetsAllRepresentationsNamesStatement;

    private PreparedStatement hasProvidedRepresentationNameStatement;

    private PreparedStatement addDataSetsRevisionStatement;

    private PreparedStatement getDataSetsRevisionStatement;

    private PreparedStatement removeDataSetsRevisionStatement;

    @PostConstruct
    private void prepareStatements() {
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
                        "INTO data_set_assignments_by_data_set (provider_dataset_id, bucket_id, schema_id, cloud_id, version_id, creation_date) " +
                        "VALUES (?,?,?,?,?,?);"
        );

        addAssignmentByRepresentationStatement = connectionProvider.getSession().prepare(
                "INSERT " +
                        "INTO data_set_assignments_by_representations (cloud_id, schema_id, version_id, provider_dataset_id, creation_date) " +
                        "VALUES (?,?,?,?,?);"
        );

        removeAssignmentStatement = connectionProvider.getSession().prepare(
                "DELETE " +
                        "FROM data_set_assignments_by_data_set " +
                        "WHERE provider_dataset_id = ? AND bucket_id = ? AND schema_id = ? AND cloud_id = ? AND version_id = ? IF EXISTS;"
        );

        removeAssignmentByRepresentationsStatement = connectionProvider.getSession().prepare(
                "DELETE " +
                        "FROM data_set_assignments_by_representations " +
                        "WHERE cloud_id = ? AND schema_id = ? AND version_id = ? AND provider_dataset_id = ? IF EXISTS;"
        );

        listDataSetRepresentationsStatement = connectionProvider.getSession().prepare(
                "SELECT cloud_id, schema_id, version_id " +
                        "FROM data_set_assignments_by_data_set " +
                        "WHERE provider_dataset_id = ? AND bucket_id = ? " +
                        "LIMIT ?;"
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

        getDataSetsForRepresentationVersionStatement = connectionProvider.getSession().prepare(
                "SELECT provider_dataset_id " +
                        "FROM data_set_assignments_by_representations " +
                        "WHERE cloud_id = ? AND schema_id = ? AND version_id= ?;"
        );

        getOneDataSetForRepresentationStatement = connectionProvider.getSession().prepare(
                "SELECT provider_dataset_id " +
                        "FROM data_set_assignments_by_representations " +
                        "WHERE cloud_id = ? AND schema_id = ? LIMIT 1;"
        );

        getDataSetsRepresentationsNamesListStatement = connectionProvider.getSession().prepare(
                "SELECT representation_names " +
                        "FROM data_set_representation_names " +
                        "WHERE provider_id = ? and dataset_id = ?;"
        );

        addDataSetsRepresentationNameStatement = connectionProvider.getSession().prepare(
                "UPDATE data_set_representation_names " +
                        "SET representation_names = representation_names + ? " +
                        "WHERE provider_id = ? and dataset_id = ?"
        );

        removeDataSetsRepresentationNameStatement = connectionProvider.getSession().prepare(
                "UPDATE data_set_representation_names " +
                        "SET representation_names = representation_names - ? " +
                        "WHERE provider_id = ? and dataset_id = ?;"
        );

        removeDataSetsAllRepresentationsNamesStatement = connectionProvider.getSession().prepare(
                "DELETE " +
                        "FROM data_set_representation_names " +
                        "WHERE provider_id = ? and dataset_id = ?;"
        );

        hasProvidedRepresentationNameStatement = connectionProvider.getSession().prepare(
                "SELECT schema_id, cloud_id " +
                        "FROM data_set_assignments_by_data_set " +
                        "WHERE provider_dataset_id = ? AND bucket_id = ? AND schema_id = ? " +
                        "LIMIT 1;"
        );

        addDataSetsRevisionStatement = connectionProvider.getSession().prepare(
                "INSERT " +
                        "INTO data_set_assignments_by_revision_id_v1 (provider_id, dataset_id, bucket_id, " +
                                "revision_provider_id, revision_name, revision_timestamp, " +
                                "representation_id, cloud_id, published, acceptance, mark_deleted) " +
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?);"
        );

        removeDataSetsRevisionStatement = connectionProvider.getSession().prepare(
                "DELETE " +
                        "FROM data_set_assignments_by_revision_id_v1 " +
                        "WHERE provider_id = ? " +
                            "AND dataset_id = ? " +
                            "AND bucket_id = ? " +
                            "AND revision_provider_id = ? " +
                            "AND revision_name = ? " +
                            "AND revision_timestamp = ? " +
                            "AND representation_id = ? " +
                            "AND cloud_id = ? " +
                        "IF EXISTS;"
        );

        getDataSetsRevisionStatement = connectionProvider.getSession().prepare(//
                "SELECT cloud_id, published, acceptance, mark_deleted " +
                        "FROM data_set_assignments_by_revision_id_v1 " +
                        "WHERE provider_id = ? " +
                            "AND dataset_id = ? " +
                            "AND bucket_id = ? " +
                            "AND revision_provider_id = ? " +
                            "AND revision_name = ? " +
                            "AND revision_timestamp = ? " +
                            "AND representation_id = ? " +
                        "LIMIT ?;"
        );
    }

    public ResultSlice<DatasetAssignment> getDatasetAssignments(String providerDataSetId, String bucketId, PagingState state, int limit) {
        List<DatasetAssignment> assignments=new ArrayList<>();
        // bind parameters, set limit to max int value
        BoundStatement boundStatement = listDataSetRepresentationsStatement.bind(
                providerDataSetId, UUID.fromString(bucketId), Integer.MAX_VALUE);

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

        if (assignments.size() == limit) {
            String nextSlice = Optional.ofNullable(rs.getExecutionInfo().getPagingState())
                    .map(Object::toString).orElse(null);
            return new ResultSlice<>(nextSlice, assignments);
        }else{
            return new ResultSlice<>(null, assignments);
        }

    }

    public void addAssignmentByRepresentationVersion(String providerDataSetId, String schema, String recordId, UUID versionId, Date timestamp) {
        BoundStatement boundStatement = addAssignmentByRepresentationStatement.bind(recordId, schema, versionId, providerDataSetId, timestamp);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    public void addAssignment(String providerId, String dataSetId, String bucketId, String recordId, String schema, Date now, UUID versionId) {
        String providerDataSetId = createProviderDataSetId(providerId, dataSetId);
        BoundStatement boundStatement = addAssignmentStatement.bind(
                providerDataSetId, UUID.fromString(bucketId), schema, recordId, versionId, now);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    /**
     * Returns data sets to which representation is assigned to.
     *
     * @param cloudId  record id
     * @param schemaId representation schema
     * @param version  representation version (might be null)
     * @return list of data set ids
     */
    public Collection<CompoundDataSetId> getDataSetAssignments(String cloudId, String schemaId, String version)
            throws NoHostAvailableException, QueryExecutionException {

        BoundStatement boundStatement = getDataSetsForRepresentationVersionStatement.bind(
                cloudId, schemaId, UUID.fromString(version));

        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        List<CompoundDataSetId> ids = new ArrayList<>();
        for (Row r : rs) {
            String providerDataSetId = r.getString(PROVIDER_DATASET_ID);
            ids.add(createCompoundDataSetId(providerDataSetId));
        }
        return ids;
    }

    /**
     * Returns one (first) data set to which representation (regardless version) is assigned to.
     *
     * @param cloudId  record id
     * @param schemaId representation schema
     * @return one dataset
     */
    public Optional<CompoundDataSetId> getOneDataSetFor(String cloudId, String schemaId)
            throws NoHostAvailableException, QueryExecutionException {

        BoundStatement boundStatement = getOneDataSetForRepresentationStatement.bind(cloudId, schemaId);

        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        Row row = rs.one();
        if (row != null) {
            String providerDataSetId = row.getString(PROVIDER_DATASET_ID);
            return Optional.of(createCompoundDataSetId(providerDataSetId));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Returns data set from specified provider with specified id. Throws
     * exception when provider does not exist. Returns null if provider exists
     * but does not have data set with specified id.
     *
     * @param providerId data set owner's (provider's) id
     * @param dataSetId  data set id
     * @return data set
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

    public boolean removeDatasetAssignment(String recordId, String schema, String versionId, String providerDataSetId, Bucket bucket) {
        BoundStatement boundStatement = removeAssignmentStatement.bind(
                providerDataSetId, UUID.fromString(bucket.getBucketId()), schema, recordId, UUID.fromString(versionId));
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        return rs.wasApplied();
    }

    public void removeAssignmentByRepresentation(String providerDataSetId, String cloudId, String schema, String versionId) {
        BoundStatement boundStatement = removeAssignmentByRepresentationsStatement.bind(
                cloudId, schema, UUID.fromString(versionId), providerDataSetId);

        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    /**
     * Creates or updates data set for a provider.
     *
     * @param providerId   data set owner's (provider's) id
     * @param dataSetId    data set id
     * @param description  description of data set.
     * @param creationTime creation date
     * @return created (or updated) data set.
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
     * @param providerId         data set owner's (provider's) id
     * @param thresholdDatasetId parameter used to pagination, returned representations wil
     *                           have dataSetId >= thresholdDatasetId. Might be null.
     * @param limit              max size of returned data set list.
     * @return list of data sets.
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
     * @param dataSetId  data set id
     */
    public void deleteDataSet(String providerId, String dataSetId) throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = deleteDataSetStatement.bind(providerId, dataSetId);
        connectionProvider.getSession().execute(boundStatement);
    }

    public Set<String> getAllRepresentationsNamesForDataSet(String providerId, String dataSetId) {
        BoundStatement boundStatement = getDataSetsRepresentationsNamesListStatement.bind(providerId, dataSetId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        Row row = rs.one();
        if (row == null) {
            return Collections.emptySet();
        } else {
            return row.getSet("representation_names", String.class);
        }
    }

    public void addDataSetsRepresentationName(String providerId, String dataSetId, String representationName) {
        Set<String> sample = new HashSet<>();
        sample.add(representationName);
        BoundStatement boundStatement = addDataSetsRepresentationNameStatement.bind(sample, providerId, dataSetId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    public void removeRepresentationNameForDataSet(String representationName, String providerId, String dataSetId) {
        Set<String> sample = new HashSet<>();
        sample.add(representationName);
        BoundStatement boundStatement = removeDataSetsRepresentationNameStatement.bind(sample, providerId, dataSetId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    public void removeAllRepresentationsNamesForDataSet(String providerId, String dataSetId) {
        BoundStatement boundStatement = removeDataSetsAllRepresentationsNamesStatement.bind(providerId, dataSetId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    public boolean datasetBucketHasAnyAssignment(String representationName, String providerDatasetId, Bucket bucket) {
        BoundStatement boundStatement = hasProvidedRepresentationNameStatement.bind(
                providerDatasetId, UUID.fromString(bucket.getBucketId()), representationName);

        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        return rs.one() != null;
    }

    public void addDataSetsRevision(String providerId, String datasetId, String bucketId, Revision revision, String representationName, String cloudId) {
        BoundStatement boundStatement = addDataSetsRevisionStatement.bind(
                providerId, datasetId, UUID.fromString(bucketId), revision.getRevisionProviderId(),
                revision.getRevisionName(), revision.getCreationTimeStamp(), representationName, cloudId,
                revision.isPublished(), revision.isAcceptance(), revision.isDeleted());

        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    public boolean removeDataSetRevision(String providerId, String datasetId, String bucketId, Revision revision, String representationName, String cloudId) {
        BoundStatement boundStatement = removeDataSetsRevisionStatement.bind(
                providerId, datasetId, UUID.fromString(bucketId), revision.getRevisionProviderId(),
                revision.getRevisionName(), revision.getCreationTimeStamp(), representationName, cloudId);

        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        return rs.wasApplied();
    }

    public ResultSlice<CloudTagsResponse> getDataSetsRevisions(String providerId, String dataSetId, String bucketId, String revisionProviderId,
                                                               String revisionName, Date revisionTimestamp, String representationName,
                                                               PagingState state, int limit){
        List<CloudTagsResponse> result=new ArrayList<>();
        // bind parameters, set limit to max int value
        BoundStatement boundStatement = getDataSetsRevisionStatement.bind(
                providerId, dataSetId, UUID.fromString(bucketId), revisionProviderId, revisionName,
                revisionTimestamp, representationName, Integer.MAX_VALUE);

        // limit page to "limit" number of results
        boundStatement.setFetchSize(limit);
        // when this is not a first page call set paging state in the statement
        if (state != null) {
            boundStatement.setPagingState(state);
        }
        // execute query
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        PagingState ps = rs.getExecutionInfo().getPagingState();
        QueryTracer.logConsistencyLevel(boundStatement, rs);

        // get available results
        Iterator<Row> iterator = rs.iterator();
        while(iterator.hasNext()){
            Row row = iterator.next();
            result.add(new CloudTagsResponse(row.getString("cloud_id"),row.getBool("published"),
                  row.getBool("mark_deleted"),row.getBool("acceptance")));

            if (result.size() >= limit){
                break;
            }
        }

        if ((result.size() == limit) && !rs.isExhausted()) {
            // we reached the page limit, prepare the next slice string to be used for the next page
            return new ResultSlice<>(ps.toString(), result);
        } else {
            return new ResultSlice<>(null, result);
        }
    }

}
