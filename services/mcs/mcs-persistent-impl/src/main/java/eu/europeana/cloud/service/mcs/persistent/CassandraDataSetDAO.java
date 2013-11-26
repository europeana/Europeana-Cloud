package eu.europeana.cloud.service.mcs.persistent;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

/**
 *
 * @author sielski
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


	@PostConstruct
	private void prepareStatements() {
		createDataSetStatement = connectionProvider.getSession().prepare(
				"UPDATE data_providers SET data_sets[?] = ? WHERE provider_id = ?;");

		deleteDataSetStatement = connectionProvider.getSession().prepare(
				"DELETE data_sets[?] FROM data_providers WHERE provider_id = ?;");

		addAssignmentStatement = connectionProvider.getSession().prepare(
				"INSERT INTO data_set_assignments (provider_dataset_id, cloud_id, schema_id, version_id, creation_date) VALUES (?,?,?,?,?);");

		removeAssignmentStatement = connectionProvider.getSession().prepare(
				"DELETE FROM data_set_assignments WHERE provider_dataset_id = ? AND cloud_id = ? AND schema_id = ?;");

		listDataSetAssignmentsNoPaging = connectionProvider.getSession().prepare(
				"SELECT * FROM data_set_assignments WHERE provider_dataset_id = ?;");

		listDataSetRepresentationsStatement = connectionProvider.getSession().prepare(
				"SELECT * FROM data_set_assignments WHERE provider_dataset_id = ? AND token(cloud_id) >= token(?) AND schema_id >= ? LIMIT ? ALLOW FILTERING;");

		listDataSetsStatement = connectionProvider.getSession().prepare(
				"SELECT data_sets FROM data_providers WHERE provider_id = ?;");
	}


	/**
	 * Returns stubs of representations assigned to a data set. Stubs contain cloud id and schema of the representation,
	 * may also contain version (if a certain version is in a data set).
	 *
	 * @param providerId
	 * @param dataSetId
	 * @param thresholdCloudId
	 * @param thresholdSchema
	 * @param limit
	 * @return
	 */
	public List<Representation> listDataSet(String providerId, String dataSetId, String thresholdCloudId, String thresholdSchema, int limit) {
		if (thresholdCloudId == null) {
			thresholdCloudId = "";
		}
		if (thresholdSchema == null) {
			thresholdSchema = "";
		}
		String providerDataSetId = createProviderDataSetId(providerId, dataSetId);
		BoundStatement boundStatement = listDataSetRepresentationsStatement.
				bind(providerDataSetId, thresholdCloudId, thresholdSchema, limit);
		ResultSet rs = connectionProvider.getSession().execute(boundStatement);
		List<Representation> representationStubs = new ArrayList<>(limit);
		for (Row row : rs) {
			Representation stub = mapRowToRepresentationStub(row);
			representationStubs.add(stub);
		}
		return representationStubs;
	}


	public void addAssignment(String providerId, String dataSetId, String recordId, String schema, String version) {
		Date now = new Date();
		String providerDataSetId = createProviderDataSetId(providerId, dataSetId);
		UUID versionId = null;
		if (version != null) {
			versionId = UUID.fromString(version);
		}
		BoundStatement boundStatement = addAssignmentStatement.bind(providerDataSetId, recordId, schema, versionId, now);
		connectionProvider.getSession().execute(boundStatement);
	}


	public DataSet getDataSet(String providerId, String dataSetId)
			throws ProviderNotExistsException {
		BoundStatement boundStatement = listDataSetsStatement.bind(providerId);
		ResultSet rs = connectionProvider.getSession().execute(boundStatement);
		Row row = rs.one();
		if (row == null) {
			throw new ProviderNotExistsException();
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


	public void removeAssignment(String providerId, String dataSetId, String recordId, String schema) {
		String providerDataSetId = createProviderDataSetId(providerId, dataSetId);
		BoundStatement boundStatement = removeAssignmentStatement.bind(providerDataSetId, recordId, schema);
		connectionProvider.getSession().execute(boundStatement);
	}


	public DataSet createDataSet(String providerId, String dataSetId, String description) {
		BoundStatement boundStatement = createDataSetStatement.bind(dataSetId, description, providerId);
		connectionProvider.getSession().execute(boundStatement);

		DataSet ds = new DataSet();
		ds.setId(dataSetId);
		ds.setDescription(description);
		ds.setProviderId(providerId);
		return ds;
	}


	public List<DataSet> getDataSets(String providerId, String thresholdDatasetId, int limit) {
		BoundStatement boundStatement = listDataSetsStatement.bind(providerId);
		ResultSet rs = connectionProvider.getSession().execute(boundStatement);
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


	public void deleteDataSet(String providerId, String dataSetId) {
		// remove all assignments
		String providerDataSetId = createProviderDataSetId(providerId, dataSetId);
		BoundStatement boundStatement = listDataSetAssignmentsNoPaging.bind(providerDataSetId);
		ResultSet rs = connectionProvider.getSession().execute(boundStatement);
		for (Row row : rs) {
			String cloudId = row.getString("cloud_id");
			String schemaId = row.getString("schema_id");
			connectionProvider.getSession().
					execute(removeAssignmentStatement.bind(providerDataSetId, cloudId, schemaId));
		}

		// remove dataset itself
		boundStatement = deleteDataSetStatement.bind(dataSetId, providerId);
		connectionProvider.getSession().execute(boundStatement);
	}


	private String createProviderDataSetId(String providerId, String dataSetId) {
		return providerId + "\n" + dataSetId;
	}


//	private DataSet mapRowToDataSet(Row row) {
//		DataSet ds = new DataSet();
//		ds.setId(row.getString("dataset_id"));
//		ds.setProviderId(row.getString("provider_id"));
//		ds.setDescription(row.getString("description"));
//		return ds;
//	}
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
