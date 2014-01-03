package eu.europeana.cloud.service.mcs.persistent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.google.common.base.Objects;

import eu.europeana.cloud.common.exceptions.ProviderDoesNotExistException;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;

/**
 * Data set repository that uses Cassandra nosql database.
 */
@Repository
public class CassandraDataSetDAO {

    @Autowired
    private CassandraConnectionProvider connectionProvider;

    private PreparedStatement createDataSetStatement;

    private PreparedStatement deleteDataSetStatement;

    private PreparedStatement addAssignmentStatement;

    private PreparedStatement removeAssignmentStatement;

    private PreparedStatement listDataSetAssignmentsNoPaging;

    private PreparedStatement listDataSetRepresentationsStatement;

    private PreparedStatement listDataSetsStatement;

    private PreparedStatement getDataSetsForRepresentationStatement;


    @PostConstruct
    private void prepareStatements() {
        createDataSetStatement = connectionProvider.getSession().prepare(
            "UPDATE data_providers SET data_sets[?] = ? WHERE provider_id = ?;");
        createDataSetStatement.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        deleteDataSetStatement = connectionProvider.getSession().prepare(
            "DELETE data_sets[?] FROM data_providers WHERE provider_id = ?;");
        deleteDataSetStatement.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        addAssignmentStatement = connectionProvider
                .getSession()
                .prepare(
                    "INSERT INTO data_set_assignments (provider_dataset_id, cloud_id, schema_id, version_id, creation_date) VALUES (?,?,?,?,?);");
        addAssignmentStatement.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        removeAssignmentStatement = connectionProvider.getSession().prepare(
            "DELETE FROM data_set_assignments WHERE provider_dataset_id = ? AND cloud_id = ? AND schema_id = ?;");
        removeAssignmentStatement.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        listDataSetAssignmentsNoPaging = connectionProvider.getSession().prepare(
            "SELECT * FROM data_set_assignments WHERE provider_dataset_id = ?;");
        listDataSetAssignmentsNoPaging.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        listDataSetRepresentationsStatement = connectionProvider
                .getSession()
                .prepare(
                    "SELECT * FROM data_set_assignments WHERE provider_dataset_id = ? AND token(cloud_id) >= token(?) AND schema_id >= ? LIMIT ? ALLOW FILTERING;");
        listDataSetRepresentationsStatement.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        listDataSetsStatement = connectionProvider.getSession().prepare(
            "SELECT data_sets FROM data_providers WHERE provider_id = ?;");
        listDataSetsStatement.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        getDataSetsForRepresentationStatement = connectionProvider.getSession().prepare(
            "SELECT provider_dataset_id, version_id FROM data_set_assignments WHERE cloud_id = ? AND schema_id = ?;");
        getDataSetsForRepresentationStatement.setConsistencyLevel(connectionProvider.getConsistencyLevel());
    }


    /**
     * Returns stubs of representations assigned to a data set. Stubs contain cloud id and schema of the representation,
     * may also contain version (if a certain version is in a data set).
     * 
     * @param providerId
     *            data set owner's (provider's) id
     * @param dataSetId
     *            data set id
     * @param thresholdCloudId
     *            parameter used to pagination, returned representations wil have cloudId >= thresholdCloudId. Might be
     *            null.
     * @param thresholdSchema
     *            parameter used to pagination, returned representations wil have schema >= thresholdSchema. Might be
     *            null.
     * @param limit
     *            maximum size of returned list
     * @return
     */
    public List<Representation> listDataSet(String providerId, String dataSetId, String thresholdCloudId,
            String thresholdSchema, int limit)
            throws NoHostAvailableException, QueryExecutionException {
        if (thresholdCloudId == null) {
            thresholdCloudId = "";
        }
        if (thresholdSchema == null) {
            thresholdSchema = "";
        }
        String providerDataSetId = createProviderDataSetId(providerId, dataSetId);
        BoundStatement boundStatement = listDataSetRepresentationsStatement.bind(providerDataSetId, thresholdCloudId,
            thresholdSchema, limit);
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
     * Adds representation to a data set. Might add representation in latest persistent or specified version. Does not
     * do any kind of parameter validation - specified data set and representation version must exist before invoking
     * this method.
     * 
     * @param providerId
     *            data set owner's (provider's) id
     * @param dataSetId
     *            data set id
     * @param recordId
     *            record id
     * @param schema
     *            representation schema
     * @param version
     *            representation version (might be null if newest version is to be assigned)
     */
    public void addAssignment(String providerId, String dataSetId, String recordId, String schema, String version)
            throws NoHostAvailableException, QueryExecutionException {
        Date now = new Date();
        String providerDataSetId = createProviderDataSetId(providerId, dataSetId);
        UUID versionId = null;
        if (version != null) {
            versionId = UUID.fromString(version);
        }
        BoundStatement boundStatement = addAssignmentStatement
                .bind(providerDataSetId, recordId, schema, versionId, now);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }


    /**
     * Returns data sets to which representation (in specified or latest version) is assigned to.
     * 
     * @param cloudId
     *            record id
     * @param schemaId
     *            representation schema
     * @param version
     *            representation version (might be null)
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
     * Returns data set from specified provider with specified id. Throws exception when provider does not exist.
     * Returns null if provider exists but does not have data set with specified id.
     * 
     * @param providerId
     *            data set owner's (provider's) id
     * @param dataSetId
     *            data set id
     * @return data set
     * @throws ProviderNotExistsException
     *             specified data provider does not exist.
     */
    public DataSet getDataSet(String providerId, String dataSetId)
            throws  NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = listDataSetsStatement.bind(providerId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        Row row = rs.one();
        if (row == null) {
            return null;
        }
        Map<String, String> datasets = row.getMap("data_sets", String.class, String.class);
        if (!datasets.containsKey(dataSetId)) {
            return null;
        }
        DataSet ds = new DataSet();
        ds.setProviderId(providerId);
        ds.setId(dataSetId);
        ds.setDescription(datasets.get(dataSetId));
        return ds;
    }


    /**
     * Removes representation from data set (regardless representation version).
     * 
     * @param providerId
     *            data set owner's (provider's) id
     * @param dataSetId
     *            data set id
     * @param recordId
     *            record's id
     * @param schema
     *            representation's schema
     */
    public void removeAssignment(String providerId, String dataSetId, String recordId, String schema)
            throws NoHostAvailableException, QueryExecutionException {
        String providerDataSetId = createProviderDataSetId(providerId, dataSetId);
        BoundStatement boundStatement = removeAssignmentStatement.bind(providerDataSetId, recordId, schema);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }


    /**
     * Creates or updates data set for a provider. Data provider with specified id must exist before this method is
     * invoked. *
     * 
     * @param providerId
     *            data set owner's (provider's) id
     * @param dataSetId
     *            data set id
     * @param description
     *            description of data set.
     * @return created (or updated) data set.
     */
    public DataSet createDataSet(String providerId, String dataSetId, String description)
            throws NoHostAvailableException, QueryExecutionException {
        if (description == null) {
            description = "";
        }
        BoundStatement boundStatement = createDataSetStatement.bind(dataSetId, description, providerId);
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
     * @param providerId
     *            data set owner's (provider's) id
     * @param thresholdDatasetId
     *            parameter used to pagination, returned representations wil have dataSetId >= thresholdDatasetId. Might
     *            be null.
     * @param limit
     *            max size of returned data set list.
     * @return list of data sets.
     */
    public List<DataSet> getDataSets(String providerId, String thresholdDatasetId, int limit)
            throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = listDataSetsStatement.bind(providerId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        Row row = rs.one();
        if (row == null) {
            return null;
        }
        Map<String, String> datasets = row.getMap("data_sets", String.class, String.class);
        NavigableMap<String, String> sortedDatasets = new TreeMap(datasets);
        if (thresholdDatasetId != null) {
            sortedDatasets = sortedDatasets.tailMap(thresholdDatasetId, true);
        }
        List<DataSet> result = new ArrayList<>(Math.min(limit, sortedDatasets.size()));
        for (Map.Entry<String, String> entry : sortedDatasets.entrySet()) {
            if (result.size() >= limit) {
                break;
            }
            DataSet ds = new DataSet();
            ds.setProviderId(providerId);
            ds.setId(entry.getKey());
            ds.setDescription(entry.getValue());
            result.add(ds);
        }
        return result;
    }


    /**
     * Deletes data set with all its assignments.
     * 
     * @param providerId
     *            data set owner's (provider's) id
     * @param dataSetId
     *            data set id
     */
    public void deleteDataSet(String providerId, String dataSetId)
            throws NoHostAvailableException, QueryExecutionException {
        // remove all assignments
        String providerDataSetId = createProviderDataSetId(providerId, dataSetId);
        BoundStatement boundStatement = listDataSetAssignmentsNoPaging.bind(providerDataSetId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        for (Row row : rs) {
            String cloudId = row.getString("cloud_id");
            String schemaId = row.getString("schema_id");
            connectionProvider.getSession().execute(
                removeAssignmentStatement.bind(providerDataSetId, cloudId, schemaId));
        }

        // remove dataset itself
        boundStatement = deleteDataSetStatement.bind(dataSetId, providerId);
        connectionProvider.getSession().execute(boundStatement);
    }


    private String createProviderDataSetId(String providerId, String dataSetId) {
        return providerId + "\n" + dataSetId;
    }


    private CompoundDataSetId createCompoundDataSetId(String providerDataSetId) {
        String[] values = providerDataSetId.split("\n");
        if (values.length != 2) {
            throw new IllegalArgumentException("Cannot construct proper compound data set id from value: "
                    + providerDataSetId);
        }
        return new CompoundDataSetId(values[0], values[1]);
    }


    private Representation mapRowToRepresentationStub(Row row) {
        Representation representation = new Representation();
        representation.setRecordId(row.getString("cloud_id"));
        representation.setSchema(row.getString("schema_id"));
        UUID verisonId = row.getUUID("version_id");
        if (verisonId != null) {
            representation.setVersion(verisonId.toString());
        }

        return representation;
    }

}
