package eu.europeana.cloud.service.mcs.persistent.cassandra;


import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.google.common.base.Objects;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.CompoundDataSetId;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.CloudIdAndTimestampResponse;
import eu.europeana.cloud.service.mcs.persistent.util.QueryTracer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.util.*;

/**
 * Data set repository that uses Cassandra nosql database.
 */
@Repository
public class CassandraDataSetDAO {

    // separator between provider id and dataset id in serialized compund
    // dataset id
    protected static final String CDSID_SEPARATOR = "\n";

    private static final int MAX_PROVIDER_DATASET_BUCKET_COUNT = 210000;

    @Autowired
    @Qualifier("dbService")
    private CassandraConnectionProvider connectionProvider;

    private PreparedStatement createDataSetStatement;

    private PreparedStatement deleteDataSetStatement;

    private PreparedStatement addAssignmentStatement;

    private PreparedStatement removeAssignmentStatement;

    private PreparedStatement listDataSetAssignmentsNoPaging;

    private PreparedStatement listDataSetRepresentationsStatement;

    private PreparedStatement listDataSetsStatement;

    private PreparedStatement getDataSetStatement;

    private PreparedStatement getDataSetsForRepresentationStatement;

    private PreparedStatement getDataSetsRepresentationsNamesList;

    private PreparedStatement addDataSetsRepresentationName;

    private PreparedStatement removeDataSetsRepresentationName;

    private PreparedStatement removeDataSetsAllRepresentationsNames;

    private PreparedStatement hasProvidedRepresentationName;

    private PreparedStatement addDataSetsRevision;

    private PreparedStatement getDataSetsRevisionWithPagination;

    private PreparedStatement getDataSetsRevision;

    private PreparedStatement getDataSetsForVersionStatement;

    private PreparedStatement removeDataSetsRevision;

    private PreparedStatement listDataSetRevisionAssignmentsNoPaging;

    private PreparedStatement getDataSetCloudIdsByRepresentationPublished;

    private PreparedStatement getDataSetCloudIdsAndTimestampsByRevisionAndRepresentation;

    private PreparedStatement insertProviderDatasetRepresentationInfo;

    private PreparedStatement deleteProviderDatasetRepresentationInfo;

    private PreparedStatement listDataSetCloudIdsByRepresentationNoPaging;

    private PreparedStatement getNextProviderDatasetBucket;

    private PreparedStatement getFirstProviderDatasetBucket;

    private PreparedStatement deleteProviderDatasetBuckets;

    private PreparedStatement getProviderDatasetBucketCount;

    private PreparedStatement updateProviderDatasetBuckets;

    private PreparedStatement decreaseProviderDatasetBuckets;

    @PostConstruct
    private void prepareStatements() {
        createDataSetStatement = connectionProvider
                .getSession()
                .prepare( //
                        "INSERT INTO " //
                                + "data_sets(provider_id, dataset_id, description, creation_date) " //
                                + "VALUES (?,?,?,?);");
        createDataSetStatement.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());

        deleteDataSetStatement = connectionProvider.getSession().prepare( //
                "DELETE FROM " //
                        + "data_sets " //
                        + "WHERE provider_id = ? AND dataset_id = ?;");
        deleteDataSetStatement.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());

        addAssignmentStatement = connectionProvider
                .getSession()
                .prepare( //
                        "INSERT INTO " //
                                + "data_set_assignments (provider_dataset_id, cloud_id, schema_id, version_id, creation_date) " //
                                + "VALUES (?,?,?,?,?);");
        addAssignmentStatement.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());

        removeAssignmentStatement = connectionProvider
                .getSession()
                .prepare( //
                        "DELETE FROM " //
                                + "data_set_assignments " //
                                + "WHERE provider_dataset_id = ? AND cloud_id = ? AND schema_id = ? AND version_id = ?;");
        removeAssignmentStatement.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());

        listDataSetAssignmentsNoPaging = connectionProvider.getSession()
                .prepare( //
                        "SELECT " //
                                + "cloud_id, schema_id, version_id " //
                                + "FROM data_set_assignments " //
                                + "WHERE provider_dataset_id = ?;");
        listDataSetAssignmentsNoPaging.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());

        listDataSetRepresentationsStatement = connectionProvider
                .getSession()
                .prepare( //
                        "SELECT " //
                                + "cloud_id, schema_id, version_id  " //
                                + "FROM data_set_assignments " //
                                + "WHERE provider_dataset_id = ? AND token(cloud_id) >= token(?) AND schema_id >= ? "
                                + "LIMIT ?;");
        listDataSetRepresentationsStatement
                .setConsistencyLevel(connectionProvider.getConsistencyLevel());

        listDataSetsStatement = connectionProvider.getSession().prepare(//
                "SELECT "//
                        + "provider_id, dataset_id, description "//
                        + "FROM data_sets "//
                        + "WHERE provider_id = ? AND dataset_id >= ? LIMIT ?;");
        listDataSetsStatement.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());

        getDataSetStatement = connectionProvider.getSession().prepare(//
                "SELECT "//
                        + "provider_id, dataset_id, description "//
                        + "FROM data_sets "//
                        + "WHERE provider_id = ? AND dataset_id = ?;");
        getDataSetStatement.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());

        getDataSetsForRepresentationStatement = connectionProvider.getSession()
                .prepare(//
                        "SELECT "//
                                + "provider_dataset_id, version_id "//
                                + "FROM data_set_assignments "//
                                + "WHERE cloud_id = ? AND schema_id = ?;");
        getDataSetsForRepresentationStatement
                .setConsistencyLevel(connectionProvider.getConsistencyLevel());

        getDataSetsRepresentationsNamesList = connectionProvider.getSession()
                .prepare(
                        "SELECT representation_names FROM data_set_representation_names where provider_id = ? and dataset_id = ?;");
        getDataSetsRepresentationsNamesList
                .setConsistencyLevel(connectionProvider.getConsistencyLevel());

        addDataSetsRepresentationName = connectionProvider.getSession()
                .prepare(
                        "UPDATE data_set_representation_names SET representation_names = representation_names + ? WHERE provider_id = ? and dataset_id = ?");
        addDataSetsRepresentationName
                .setConsistencyLevel(connectionProvider.getConsistencyLevel());

        removeDataSetsRepresentationName = connectionProvider.getSession()
                .prepare(
                        "UPDATE data_set_representation_names SET representation_names = representation_names - ? WHERE provider_id = ? and dataset_id = ?;");
        removeDataSetsRepresentationName
                .setConsistencyLevel(connectionProvider.getConsistencyLevel());

        removeDataSetsAllRepresentationsNames = connectionProvider.getSession()
                .prepare(
                        "DELETE FROM data_set_representation_names where provider_id = ? and dataset_id = ?;");
        removeDataSetsAllRepresentationsNames
                .setConsistencyLevel(connectionProvider.getConsistencyLevel());

        hasProvidedRepresentationName = connectionProvider.getSession()
                .prepare(
                        "SELECT " //
                                + "cloud_id, schema_id " //
                                + "FROM data_set_assignments " //
                                + "WHERE provider_dataset_id = ? AND schema_id = ? LIMIT 1;");
        hasProvidedRepresentationName
                .setConsistencyLevel(connectionProvider.getConsistencyLevel());

        addDataSetsRevision = connectionProvider
                .getSession()
                .prepare( //
                        "INSERT INTO " //
                                + "data_set_assignments_by_revision_id (provider_dataset_id, revision_id, representation_id, cloud_id) " //
                                + "VALUES (?,?,?,?);");
        addDataSetsRevision.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());

        removeDataSetsRevision
                = connectionProvider.getSession().prepare(//
                "DELETE "//
                        + "FROM data_set_assignments_by_revision_id "//
                        + "WHERE provider_dataset_id = ? AND revision_id = ? AND representation_id = ? " +
                        "AND cloud_id = ?;");
        removeDataSetsRevision
                .setConsistencyLevel(connectionProvider.getConsistencyLevel());

        getDataSetsRevision = connectionProvider.getSession().prepare(//
                "SELECT "//
                        + "cloud_id "//
                        + "FROM data_set_assignments_by_revision_id "//
                        + "WHERE provider_dataset_id = ? AND revision_id = ? AND representation_id = ? LIMIT ?;");
        getDataSetsRevision
                .setConsistencyLevel(connectionProvider.getConsistencyLevel());

        getDataSetsRevisionWithPagination = connectionProvider.getSession().prepare(//
                "SELECT "//
                        + "cloud_id "//
                        + "FROM data_set_assignments_by_revision_id "//
                        + "WHERE provider_dataset_id = ? AND revision_id = ? AND representation_id = ? AND cloud_id >= ? LIMIT ?; ");
        getDataSetsRevisionWithPagination
                .setConsistencyLevel(connectionProvider.getConsistencyLevel());

        listDataSetRevisionAssignmentsNoPaging = connectionProvider.getSession()
                .prepare( //
                        "SELECT " //
                                + "revision_id, representation_id, cloud_id " //
                                + "FROM data_set_assignments_by_revision_id " //
                                + "WHERE provider_dataset_id = ?;");
        listDataSetRevisionAssignmentsNoPaging.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());

        getDataSetsForVersionStatement = connectionProvider.getSession().prepare(//
                "SELECT "//
                        + "provider_dataset_id "//
                        + "FROM data_set_assignments "//
                        + "WHERE cloud_id = ? AND schema_id = ? AND version_id = ?;");
        getDataSetsForVersionStatement
                .setConsistencyLevel(connectionProvider.getConsistencyLevel());

        listDataSetCloudIdsByRepresentationNoPaging = connectionProvider.getSession()
                .prepare( //
                        "SELECT " //
                                + "representation_id, revision_timestamp, revision_id, cloud_id " //
                                + "FROM provider_dataset_representation " //
                                + "WHERE provider_id = ? AND dataset_id = ? AND bucket_id = ?;");
        listDataSetCloudIdsByRepresentationNoPaging.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());

        getDataSetCloudIdsByRepresentationPublished = connectionProvider.getSession().prepare("SELECT " //
                + "cloud_id, version_id, revision_id, published, mark_deleted, acceptance " //
                + "FROM provider_dataset_representation " //
                + "WHERE provider_id = ? AND dataset_id = ? AND bucket_id = ? AND representation_id = ? AND revision_timestamp > ? AND published = true LIMIT ?;");
        getDataSetCloudIdsByRepresentationPublished.setConsistencyLevel(connectionProvider.getConsistencyLevel());


        getDataSetCloudIdsAndTimestampsByRevisionAndRepresentation = connectionProvider.getSession().prepare("SELECT cloud_id, revision_timestamp " //
                + "FROM provider_dataset_representation " //
                + "WHERE provider_id = ? AND dataset_id = ? AND bucket_id = ? AND revision_id = ? And representation_id = ? AND revision_timestamp > ?  LIMIT ?;");
        getDataSetCloudIdsAndTimestampsByRevisionAndRepresentation.setConsistencyLevel(connectionProvider.getConsistencyLevel());


        insertProviderDatasetRepresentationInfo = connectionProvider.getSession().prepare("INSERT INTO " //
                + "provider_dataset_representation (provider_id, dataset_id, bucket_id, cloud_id, version_id, representation_id," //
                + "revision_id, revision_timestamp, acceptance, published, mark_deleted) " //
                + "VALUES (?,?,?,?,?,?,?,?,?,?,?);");
        insertProviderDatasetRepresentationInfo.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        deleteProviderDatasetRepresentationInfo = connectionProvider.getSession().prepare(//
                "DELETE FROM " //
                        + "provider_dataset_representation " //
                        + "WHERE provider_id = ? AND dataset_id = ? AND bucket_id = ? AND representation_id = ? AND revision_timestamp = ? AND cloud_id = ?;");
        deleteProviderDatasetRepresentationInfo.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        updateProviderDatasetBuckets = connectionProvider.getSession().prepare("UPDATE datasets_buckets " //
                + "SET rows_count = rows_count + 1 WHERE provider_id = ? AND dataset_id = ? AND bucket_id = ?;");
        updateProviderDatasetBuckets.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        decreaseProviderDatasetBuckets = connectionProvider.getSession().prepare("UPDATE datasets_buckets " //
                + "SET rows_count = rows_count - 1 WHERE provider_id = ? AND dataset_id = ? AND bucket_id = ?;");
        decreaseProviderDatasetBuckets.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        getProviderDatasetBucketCount = connectionProvider.getSession().prepare("SELECT bucket_id, rows_count " //
                + "FROM datasets_buckets " //
                + "WHERE provider_id = ? AND dataset_id = ?;");
        getProviderDatasetBucketCount.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        getNextProviderDatasetBucket = connectionProvider.getSession().prepare("SELECT bucket_id " //
                + "FROM datasets_buckets " //
                + "WHERE provider_id = ? AND dataset_id = ? AND bucket_id > ? LIMIT 1;");
        getNextProviderDatasetBucket.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        getFirstProviderDatasetBucket = connectionProvider.getSession().prepare("SELECT bucket_id " //
                + "FROM datasets_buckets " //
                + "WHERE provider_id = ? AND dataset_id = ? LIMIT 1;");
        getFirstProviderDatasetBucket.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        deleteProviderDatasetBuckets = connectionProvider.getSession().prepare("DELETE FROM " //
                + "datasets_buckets "
                + "WHERE provider_id = ? AND dataset_id = ? AND bucket_id = ?;");
        deleteProviderDatasetBuckets.setConsistencyLevel(connectionProvider.getConsistencyLevel());
    }

    /**
     * Returns stubs of representations assigned to a data set. Stubs contain
     * cloud id and schema of the representation, may also contain version (if a
     * certain version is in a data set).
     *
     * @param providerId       data set owner's (provider's) id
     * @param dataSetId        data set id
     * @param thresholdCloudId parameter used to pagination, returned representations wil
     *                         have cloudId >= thresholdCloudId. Might be null.
     * @param thresholdSchema  parameter used to pagination, returned representations wil
     *                         have schema >= thresholdSchema. Might be null.
     * @param limit            maximum size of returned list
     * @return
     */
    public List<Representation> listDataSet(String providerId, String dataSetId, String thresholdCloudId,
                                            String thresholdSchema, int limit)
            throws NoHostAvailableException, QueryExecutionException {
        String providerDataSetId = createProviderDataSetId(providerId, dataSetId);
        BoundStatement boundStatement = listDataSetRepresentationsStatement.bind(providerDataSetId,
                thresholdCloudId != null ? thresholdCloudId : "", thresholdSchema != null ? thresholdSchema : "", limit);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        List<Representation> representationStubs = new ArrayList<>(limit);
        for (Row row : rs) {
            Representation stub = mapRowToRepresentationStub(row);
            representationStubs.add(stub);
        }
        return representationStubs;
    }

    /**
     * Adds representation to a data set. Might add representation in latest
     * persistent or specified version. Does not do any kind of parameter
     * validation - specified data set and representation version must exist
     * before invoking this method.
     *
     * @param providerId data set owner's (provider's) id
     * @param dataSetId  data set id
     * @param recordId   record id
     * @param schema     representation schema
     * @param version    representation version (might be null if newest version is to
     *                   be assigned)
     */
    public void addAssignment(String providerId, String dataSetId,
                              String recordId, String schema, String version)
            throws NoHostAvailableException, QueryExecutionException {
        Date now = new Date();
        String providerDataSetId = createProviderDataSetId(providerId,
                dataSetId);
        UUID versionId = null;
        if (version != null) {
            versionId = UUID.fromString(version);
        }
        BoundStatement boundStatement = addAssignmentStatement.bind(
                providerDataSetId, recordId, schema, versionId, now);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    /**
     * Returns data sets to which representation (in specified or latest
     * version) is assigned to.
     *
     * @param cloudId  record id
     * @param schemaId representation schema
     * @param version  representation version (might be null)
     * @return list of data set ids
     */
    public Collection<CompoundDataSetId> getDataSetAssignments(String cloudId, String schemaId, String version)
            throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = getDataSetsForRepresentationStatement.bind(cloudId, schemaId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        List<CompoundDataSetId> ids = new ArrayList<>();
        for (Row r : rs) {
            UUID versionId = r.getUUID("version_id");
            String versionIdString = versionId == null ? null : versionId.toString();
            if (Objects.equal(version, versionIdString)) {
                String providerDataSetId = r.getString("provider_dataset_id");
                ids.add(createCompoundDataSetId(providerDataSetId));
            }
        }
        return ids;
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
    public DataSet getDataSet(String providerId, String dataSetId)
            throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = getDataSetStatement.bind(providerId,
                dataSetId);
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
     * Removes representation from data set (regardless representation version).
     *
     * @param providerId data set owner's (provider's) id
     * @param dataSetId  data set id
     * @param recordId   record's id
     * @param schema     representation's schema
     */
    public void removeAssignment(String providerId, String dataSetId,
                                 String recordId, String schema, String versionId) throws NoHostAvailableException,
            QueryExecutionException {
        String providerDataSetId = createProviderDataSetId(providerId,
                dataSetId);
        BoundStatement boundStatement = removeAssignmentStatement.bind(
                providerDataSetId, recordId, schema, UUID.fromString(versionId));
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
    public DataSet createDataSet(String providerId, String dataSetId,
                                 String description, Date creationTime)
            throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = createDataSetStatement.bind(providerId,
                dataSetId, description, creationTime);
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

        BoundStatement boundStatement = listDataSetsStatement.bind(providerId,
                thresholdDatasetId != null ? thresholdDatasetId : "", limit);
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

    public Set<String> getDataSets(String providerId, String cloudId, String representationName, String version) {
        BoundStatement boundStatement = getDataSetsForVersionStatement.bind(
                cloudId,
                representationName,
                UUID.fromString(version));
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        Set<String> result = new LinkedHashSet<>();
        for (Row row : rs) {
            String provider_dataset_id = row.getString("provider_dataset_id");
            String prefix = providerId + "\n";
            if (provider_dataset_id.startsWith(prefix)) {
                result.add(provider_dataset_id.replace(prefix, ""));
            }
        }
        return result;
    }

    /**
     * Deletes data set with all its assignments.
     *
     * @param providerId data set owner's (provider's) id
     * @param dataSetId  data set id
     */
    public void deleteDataSet(String providerId, String dataSetId)
            throws NoHostAvailableException, QueryExecutionException {
        String providerDataSetId = createProviderDataSetId(providerId,
                dataSetId);

        removeAllDataSetAssignments(providerDataSetId);
        removeAllDataSetRevisonAssignments(providerDataSetId);
        removeAllDataSetCloudIdsByRepresentation(providerId, dataSetId);
        removeAllDataSetBuckets(providerId, dataSetId);

        // remove dataset itself
        BoundStatement boundStatement = deleteDataSetStatement.bind(providerId, dataSetId);
        connectionProvider.getSession().execute(boundStatement);
    }

    private void removeAllDataSetBuckets(String providerId, String dataSetId) {
        synchronized (updateProviderDatasetBuckets) {
            for (String bucket_id : getAllDatasetBuckets(providerId, dataSetId)) {
                connectionProvider.getSession().execute(
                        deleteProviderDatasetBuckets.bind(providerId, dataSetId, UUID.fromString(bucket_id)));
            }
        }
    }

    private List<String> getAllDatasetBuckets(String providerId, String dataSetId) {
        List<String> result = new ArrayList<>();
        BoundStatement boundStatement = getProviderDatasetBucketCount.bind(providerId, dataSetId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);

        for (Row row : rs) {
            result.add(row.getUUID("bucket_id").toString());
        }
        return result;
    }

    private void removeAllDataSetCloudIdsByRepresentation(String providerId, String dataSetId) {
        synchronized (updateProviderDatasetBuckets) {
            for (String bucket_id : getAllDatasetBuckets(providerId, dataSetId)) {
                removeAllDataSetCloudIdsByRepresentationBucket(providerId, dataSetId, bucket_id);
            }
        }
    }

    private void removeAllDataSetCloudIdsByRepresentationBucket(String providerId, String dataSetId, String bucket_id) {
        BoundStatement boundStatement = listDataSetCloudIdsByRepresentationNoPaging
                .bind(providerId, dataSetId, UUID.fromString(bucket_id));
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        for (Row row : rs) {
            String cloudId = row.getString("cloud_id");
            String schemaId = row.getString("representation_id");
            Date revisionTimestamp = row.getDate("revision_timestamp");
            connectionProvider.getSession().execute(
                    deleteProviderDatasetRepresentationInfo.bind(providerId, dataSetId, UUID.fromString(bucket_id), schemaId, revisionTimestamp, cloudId));
            decreaseProviderDatasetBuckets(providerId, dataSetId, bucket_id);
        }
    }

    private void removeAllDataSetAssignments(String providerDataSetId) {
        BoundStatement boundStatement = listDataSetAssignmentsNoPaging
                .bind(providerDataSetId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        for (Row row : rs) {
            String cloudId = row.getString("cloud_id");
            String schemaId = row.getString("schema_id");
            UUID versionId = row.getUUID("version_id");
            connectionProvider.getSession().execute(
                    removeAssignmentStatement.bind(providerDataSetId, cloudId,
                            schemaId, versionId));
        }
    }

    private void removeAllDataSetRevisonAssignments(String providerDataSetId) {
        BoundStatement boundStatement = listDataSetRevisionAssignmentsNoPaging
                .bind(providerDataSetId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        for (Row row : rs) {
            String revisionId = row.getString("revision_id");
            String representationId = row.getString("representation_id");
            String cloudId = row.getString("cloud_id");
            connectionProvider.getSession().execute(
                    removeDataSetsRevision.bind(providerDataSetId, revisionId,
                            representationId, cloudId));
        }
    }


    public Set<String> getAllRepresentationsNamesForDataSet(String providerId, String dataSetId) {
        BoundStatement boundStatement = getDataSetsRepresentationsNamesList.bind(providerId, dataSetId);
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
        BoundStatement boundStatement = addDataSetsRepresentationName.bind(sample, providerId, dataSetId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    public void removeRepresentationNameForDataSet(String representationName, String providerId, String dataSetId) {
        Set<String> sample = new HashSet<String>();
        sample.add(representationName);
        BoundStatement boundStatement = removeDataSetsRepresentationName.bind(sample, providerId, dataSetId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    public void removeAllRepresentationsNamesForDataSet(String providerId, String dataSetId) {
        BoundStatement boundStatement = removeDataSetsAllRepresentationsNames.bind(providerId, dataSetId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    public boolean hasMoreRepresentations(String providerId, String datasetId, String representationName) {
        String providerDatasetId = providerId + CDSID_SEPARATOR + datasetId;
        BoundStatement boundStatement = hasProvidedRepresentationName.bind(providerDatasetId, representationName);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        if (rs.one() != null) {
            return true;
        } else {
            return false;
        }
    }

    private String createProviderDataSetId(String providerId, String dataSetId) {
        return providerId + CDSID_SEPARATOR + dataSetId;
    }

    private CompoundDataSetId createCompoundDataSetId(String providerDataSetId) {
        String[] values = providerDataSetId.split(CDSID_SEPARATOR);
        if (values.length != 2) {
            throw new IllegalArgumentException(
                    "Cannot construct proper compound data set id from value: "
                            + providerDataSetId);
        }
        return new CompoundDataSetId(values[0], values[1]);
    }

    private Representation mapRowToRepresentationStub(Row row) {
        Representation representation = new Representation();
        representation.setCloudId(row.getString("cloud_id"));
        representation.setRepresentationName(row.getString("schema_id"));
        UUID verisonId = row.getUUID("version_id");
        if (verisonId != null) {
            representation.setVersion(verisonId.toString());
        }

        return representation;
    }

    public void addDataSetsRevision(String providerId, String datasetId, String revisionId, String representationName, String cloudId) {
        String providerDatasetId = createProviderDataSetId(providerId, datasetId);
        BoundStatement boundStatement = addDataSetsRevision.bind(providerDatasetId, revisionId, representationName, cloudId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    public void removeDataSetsRevision(String providerId, String datasetId, String revisionId, String
            representationName, String cloudId) {
        String providerDatasetId = createProviderDataSetId(providerId, datasetId);
        BoundStatement boundStatement = removeDataSetsRevision.bind(providerDatasetId, revisionId,
                representationName, cloudId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    public List<String> getDataSetsRevisionWithPagination(String providerId, String datasetId, String revisionId, String representationName, String startFromCloudId, int limit) {
        String providerDatasetId = createProviderDataSetId(providerId, datasetId);
        BoundStatement boundStatement = getDataSetsRevisionWithPagination.bind(providerDatasetId, revisionId, representationName, startFromCloudId, limit);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        List<String> cloudIds = new ArrayList<>();
        for (Row r : rs) {
            String cloudId = r.getString("cloud_Id");
            cloudIds.add(cloudId);
        }
        return cloudIds;
    }

    public List<String> getDataSetsRevision(String providerId, String datasetId, String revisionId, String representationName, int limit) {
        String providerDatasetId = createProviderDataSetId(providerId, datasetId);
        BoundStatement boundStatement = getDataSetsRevision.bind(providerDatasetId, revisionId, representationName, limit);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        List<String> cloudIds = new ArrayList<>();
        for (Row r : rs) {
            String cloudId = r.getString("cloud_Id");
            cloudIds.add(cloudId);
        }
        return cloudIds;
    }


    /**
     * Lists cloud identifiers of provider's data set having given representation name and revision published after a specific time. Together with cloud identifier also
     * version identifier and revision identifier are returned. All these values are packed in Properties object where keys are: cloudId, versionId, revisionId. The last element
     * of the list may contain Properties object with just one property (key is nextSlice) indicating token that may be used for next page of results.
     *
     * @param providerId         data set provider id
     * @param dataSetId          identifier of a data set
     * @param representationName representation name
     * @param dateFrom           date of last revision
     * @param nextToken          cloud identifier combined with timestamp from which to start the result list, used in pagination, may be null
     * @param limit              max size of returned cloud identifiers list.
     * @return list of Properties object where each such object contains cloud identifier, version identifier and revision identifier
     */
    public List<Properties> getDataSetCloudIdsByRepresentationPublished(String providerId, String dataSetId, String representationName, Date dateFrom, String nextToken, int limit)
            throws NoHostAvailableException, QueryExecutionException {
        List<Properties> result = new ArrayList<>(limit);

        String bucketId;
        PagingState state;

        if (nextToken == null) {
            // there is no next token so do not set paging state, take the first bucket for provider's dataset
            bucketId = getNextBucket(providerId, dataSetId, null);
            state = null;
        } else {
            // next token is set, parse it to retrieve paging state and bucket id (token is concatenation of paging state and bucket id using _ character
            String[] parts = nextToken.split("_");
            if (parts.length != 2)
                throw new IllegalArgumentException("nextToken format is wrong. nextToken = " + nextToken);

            // first element is the paging state
            state = getPagingState(parts[0]);
            // second element is bucket id
            bucketId = getBucketId(parts[1], state, providerId, dataSetId);
        }

        // if the bucket is null it means we reached the end of data
        if (bucketId == null)
            return result;

        // bind parameters, set limit to max int value
        BoundStatement boundStatement = getDataSetCloudIdsByRepresentationPublished.bind(providerId, dataSetId, UUID.fromString(bucketId), representationName, dateFrom, Integer.MAX_VALUE);
        // limit page to "limit" number of results
        boundStatement.setFetchSize(limit);
        // when this is not a first page call set paging state in the statement
        if (state != null)
            boundStatement.setPagingState(state);

        // execute query
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);

        // get available results
        int available = rs.getAvailableWithoutFetching();
        for (int i = 0; i < available; i++) {
            Row row = rs.one();
            Properties properties = new Properties();
            properties.put("cloudId", row.getString("cloud_id"));
            properties.put("versionId", row.getUUID("version_id").toString());
            properties.put("revisionId", row.getString("revision_id"));
            properties.put("published", row.getBool("published"));
            properties.put("deleted", row.getBool("mark_deleted"));
            properties.put("acceptance", row.getBool("acceptance"));
            result.add(properties);
        }

        if (result.size() == limit) {
            // we reached the page limit, prepare the next slice string to be used for the next page
            String nextSlice = getNextSlice(rs.getExecutionInfo().getPagingState(), bucketId, providerId, dataSetId);

            if (nextSlice != null) {
                Properties properties = new Properties();
                properties.put("nextSlice", nextSlice);
                result.add(properties);
            }
        } else {
            // we reached the end of bucket but number of results is less than the page size - in this case if there are more buckets we should retrieve number of results that will feed the page
            if (getNextBucket(providerId, dataSetId, bucketId) != null) {
                String nextSlice = "_" + bucketId;
                result.addAll(getDataSetCloudIdsByRepresentationPublished(providerId, dataSetId, representationName, dateFrom, nextSlice, limit - result.size()));
            }
        }

        return result;
    }


    /**
     * get the latest cloud identifier,revision timestamp that belong to data set of a specified provider for a specific representation and revision and where revision timestamp is bigger than a specified date ;
     *
     * @param providerId                  data set provider id
     * @param dataSetId                   identifier of a data set
     * @param revisionId                  revisionId
     * @param representationName          representation name
     * @param dateFrom                    date of last revision
     * @param nextToken                   it will be used to iterate over the bucket it
     * @param cloudIdAndTimestampResponse hold the latest cloud identifier,revision timestamp
     * @return get the latest cloud identifier,revision timestamp that belong to data set of a specified provider for a specific representation and revision and where revision timestamp is bigger than a specified date ;
     */
    public CloudIdAndTimestampResponse getLatestDataSetCloudIdByRepresentationAndRevision(String providerId, String dataSetId, String revisionId, String representationName, Date dateFrom, String nextToken, CloudIdAndTimestampResponse cloudIdAndTimestampResponse)
            throws NoHostAvailableException, QueryExecutionException {
        String bucketId = getNextBucket(providerId, dataSetId, nextToken);
        if (bucketId == null)
            return cloudIdAndTimestampResponse;
        ResultSet rs = getQueryResults(providerId, dataSetId, revisionId, representationName, dateFrom, bucketId);
        CloudIdAndTimestampResponse latest = getTheLatestCloudId(rs, cloudIdAndTimestampResponse);

        return getLatestDataSetCloudIdByRepresentationAndRevision(providerId, dataSetId, revisionId, representationName, dateFrom, bucketId, latest);

    }

    private CloudIdAndTimestampResponse getTheLatestCloudId(ResultSet rs, CloudIdAndTimestampResponse latest) {
        int available = rs.getAvailableWithoutFetching();
        if (available > 0 && latest.isEmpty()) {
            Row row = rs.one();
            latest.setCloudId(row.getString("cloud_id"));
            latest.setRevisionTimestamp(row.getDate("revision_timestamp"));
        }
        available = rs.getAvailableWithoutFetching();
        for (int i = 0; i < available; i++) {
            Row row = rs.one();
            Date timestamp = row.getDate("revision_timestamp");
            if (timestamp.getTime() > latest.getRevisionTimestamp().getTime()) {
                latest.setRevisionTimestamp(timestamp);
                latest.setCloudId(row.getString("cloud_id"));
            }
        }
        return latest;
    }

    private ResultSet getQueryResults(String providerId, String dataSetId, String revisionId, String representationName, Date dateFrom, String bucketId) {
        BoundStatement boundStatement = getDataSetCloudIdsAndTimestampsByRevisionAndRepresentation.bind(providerId, dataSetId, UUID.fromString(bucketId), revisionId, representationName, dateFrom, Integer.MAX_VALUE);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        return rs;
    }


    /**
     * Get next slice string basing on paging state of the current query and bucket id.
     *
     * @param pagingState paging state of the current query
     * @param bucketId    current bucket identifier
     * @param providerId  provider id needed to retrieve next bucket id
     * @param dataSetId   dataset id needed to retrieve next bucket id
     * @return next slice as the concatenation of paging state and bucket id (paging state may be empty_ or null when there are no more buckets available
     */
    private String getNextSlice(PagingState pagingState, String bucketId, String providerId, String dataSetId) {
        if (pagingState == null) {
            // we possibly reached the end of a bucket, if there are more buckets we should prepare next slice otherwise not
            if (getNextBucket(providerId, dataSetId, bucketId) != null)
                return "_" + bucketId;
        } else
            return pagingState.toString() + "_" + bucketId;
        return null;
    }

    /**
     * Get bucket id from part of token considering paging state which was retrieved from the same token
     *
     * @param tokenPart  part of token containing bucket id
     * @param state      paging state from the same token as the bucket id
     * @param providerId provider id to retrieve next bucket id
     * @param dataSetId  dataset id to retrieve next bucket id
     * @return bucket id to be used for the query
     */
    private String getBucketId(String tokenPart, PagingState state, String providerId, String dataSetId) {
        if (tokenPart != null && !tokenPart.isEmpty()) {
            // when the state passed in the next token is not null we have to use the same bucket id as the paging state is associated with the query having certain parameter values
            if (state != null)
                return tokenPart;
            else {
                // the state part is empty which means we reached the end of the bucket passed in the next token, therefore we need to get the next bucket
                return getNextBucket(providerId, dataSetId, tokenPart);
            }
        }
        return null;
    }

    /**
     * Get paging state from part of token. When the token is null or empty paging state is null. Otherwise we can create paging state from that string.
     *
     * @param tokenPart part of token containing string representation of paging state from previous query
     * @return null when token part is empty or null paging state otherwise
     */
    private PagingState getPagingState(String tokenPart) {
        if (tokenPart != null && !tokenPart.isEmpty())
            return PagingState.fromString(tokenPart);
        return null;
    }

    private String getNextBucket(String providerId, String dataSetId, String bucketId) {
        BoundStatement bs = bucketId == null ?
                getFirstProviderDatasetBucket.bind(providerId, dataSetId)
                : getNextProviderDatasetBucket.bind(providerId, dataSetId, UUID.fromString(bucketId));
        ResultSet rs = connectionProvider.getSession().execute(bs);
        QueryTracer.logConsistencyLevel(bs, rs);
        Row row = rs.one();
        // there should be only one row or none
        if (row != null)
            return row.getUUID("bucket_id").toString();
        return null;
    }

    /**
     * Insert row to provider_dataset_representation table.
     *
     * @param dataSetId         data set identifier
     * @param dataSetProviderId provider identifier
     * @param globalId          cloud identifier
     * @param schema            representation name
     * @param revisionId        revision identifier
     * @param updateTimeStamp   timestamp of revision update
     * @param acceptance        acceptance tag
     * @param published         published tag
     * @param deleted           mark deleted tag
     */
    public void insertProviderDatasetRepresentationInfo(String dataSetId, String dataSetProviderId, String globalId,
                                                        String versionId, String schema, String revisionId, Date updateTimeStamp,
                                                        boolean acceptance, boolean published, boolean deleted)
            throws NoHostAvailableException, QueryExecutionException {
        String bucketId = null;
        synchronized (updateProviderDatasetBuckets) {
            Properties bucketCount = getCurrentProviderDatasetBucket(dataSetProviderId, dataSetId);
            // when there is no bucket or bucket rows count is max we should add another bucket
            if (bucketCount.isEmpty() || Integer.valueOf(bucketCount.getProperty("rows_count")) == MAX_PROVIDER_DATASET_BUCKET_COUNT)
                bucketId = createBucket();
            else
                bucketId = bucketCount.getProperty("bucket_id");
            increaseBucketCount(dataSetProviderId, dataSetId, bucketId);
        }
        BoundStatement bs = insertProviderDatasetRepresentationInfo.bind(dataSetProviderId, dataSetId, UUID.fromString(bucketId), globalId, UUID.fromString(versionId), schema,
                revisionId, updateTimeStamp, acceptance, published, deleted);
        ResultSet rs = connectionProvider.getSession().execute(bs);
        QueryTracer.logConsistencyLevel(bs, rs);
    }


    private void increaseBucketCount(String dataSetProviderId, String dataSetId, String bucketId) {
        BoundStatement statement = updateProviderDatasetBuckets.bind(dataSetProviderId, dataSetId, UUID.fromString(bucketId));
        ResultSet rs = connectionProvider.getSession().execute(statement);
        QueryTracer.logConsistencyLevel(statement, rs);
    }


    private String createBucket() {
        return new com.eaio.uuid.UUID().toString();
    }


    public Properties getCurrentProviderDatasetBucket(String providerId, String datasetId) {
        Properties result = new Properties();
        BoundStatement rowsStatement = getProviderDatasetBucketCount.bind(providerId, datasetId);
        ResultSet rs = connectionProvider.getSession().execute(rowsStatement);
        // get last element on the list
        List<Row> rows = rs.all();
        Row row = rows.isEmpty() ? null : rows.get(rows.size() - 1);
        if (row != null) {
            result.setProperty("bucket_id", row.getUUID("bucket_id").toString());
            result.setProperty("rows_count", String.valueOf(row.getLong("rows_count")));
        }
        return result;
    }

    /**
     * Remove row from provider_dataset_representation table.
     *
     * @param dataSetId         data set identifier
     * @param dataSetProviderId provider identifier
     * @param globalId          cloud identifier
     * @param schema            representation name
     * @param updateTimeStamp   timestamp of revision update
     *                          +
     */
    public void deleteProviderDatasetRepresentationInfo(String dataSetId, String dataSetProviderId, String globalId,
                                                        String schema, Date updateTimeStamp)
            throws NoHostAvailableException, QueryExecutionException {
        synchronized (updateProviderDatasetBuckets) {
            String bucketId = getNextBucket(dataSetProviderId, dataSetId, null);
            while (bucketId != null) {
                BoundStatement bs = deleteProviderDatasetRepresentationInfo.bind(dataSetProviderId, dataSetId, UUID.fromString(bucketId), schema,
                        updateTimeStamp, globalId);
                ResultSet rs = connectionProvider.getSession().execute(bs);
                QueryTracer.logConsistencyLevel(bs, rs);
                decreaseProviderDatasetBuckets(dataSetProviderId, dataSetId, bucketId);
                bucketId = getNextBucket(dataSetProviderId, dataSetId, bucketId);
            }
        }
    }

    private void decreaseProviderDatasetBuckets(String dataSetProviderId, String dataSetId, String bucketId) {
        BoundStatement bs = decreaseProviderDatasetBuckets.bind(dataSetProviderId, dataSetId, UUID.fromString(bucketId));
        ResultSet rs = connectionProvider.getSession().execute(bs);
        QueryTracer.logConsistencyLevel(bs, rs);
    }
}
