package eu.europeana.cloud.service.mcs.persistent;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import eu.europeana.cloud.common.model.DataSet;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.DataSetAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.DataSetNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationAlreadyInSetException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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

	private PreparedStatement removeAssignmentsStatement;

	private PreparedStatement listDataSetRepresentationsStatement;

	private PreparedStatement listDataSetsStatement;


	@PostConstruct
	private void prepareStatements() {
		createDataSetStatement = connectionProvider.getSession().prepare(
				"INSERT INTO data_sets (provider_id, dataset_id, description, creation_date) VALUES (?,?,?,?) IF NOT EXISTS;");
		
		deleteDataSetStatement = connectionProvider.getSession().prepare(
				"DELETE FROM data_sets WHERE provider_id = ? AND dataset_id = ?;");
		
		addAssignmentStatement = connectionProvider.getSession().prepare(
				"INSERT INTO data_set_assignments (provider_dataset_id, cloud_id, schema_id, version, creation_date) VALUES (?,?,?,?,?) IF NOT EXISTS;");
		
		removeAssignmentStatement = connectionProvider.getSession().prepare(
				"DELETE FROM data_set_assignments WHERE provider_dataset_id = ? AND cloud_id = ? AND schema_id = ?;");
		
		removeAssignmentsStatement = connectionProvider.getSession().prepare(
				"DELETE FROM data_set_assignments WHERE provider_dataset_id = ?;");
		
		listDataSetRepresentationsStatement = connectionProvider.getSession().prepare(
				"SELECT * FROM data_set_assignments WHERE provider_dataset_id = ? AND token(cloud_id) >= token(?) AND schema_id >= ? LIMIT ? ALLOW FILTERING;");

		listDataSetsStatement = connectionProvider.getSession().prepare(
				"SELECT * FROM data_sets WHERE provider_id = ? AND dataset_id >= ? LIMIT ?");
	}


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
			representationStubs.add(mapRowToRepresentationStub(providerId, row));
		}
		return representationStubs;
	}


	public void addAssignment(String providerId, String dataSetId, String recordId, String schema, String version)
			throws RepresentationAlreadyInSetException {
		Date now = new Date();
		String providerDataSetId = createProviderDataSetId(providerId, dataSetId);
		BoundStatement boundStatement = addAssignmentStatement.bind(providerDataSetId, recordId, schema, version, now);
		ResultSet rs = connectionProvider.getSession().execute(boundStatement);
		Row result = rs.one();
		boolean applied = result.getBool("[applied]");
		if (!applied) {
			throw new RepresentationAlreadyInSetException(recordId, schema, dataSetId, providerId);
		}
	}


	public void removeAssignment(String providerId, String dataSetId, String recordId, String schema) {
		String providerDataSetId = createProviderDataSetId(providerId, dataSetId);
		BoundStatement boundStatement = removeAssignmentStatement.bind(providerDataSetId, recordId, schema);
		ResultSet rs = connectionProvider.getSession().execute(boundStatement);
	}


	public DataSet createDataSet(String providerId, String dataSetId, String description)
			throws DataSetAlreadyExistsException {
		Date now = new Date();
		BoundStatement boundStatement = createDataSetStatement.bind(providerId, dataSetId, description, now);
		ResultSet rs = connectionProvider.getSession().execute(boundStatement);
		Row row = rs.one();
		boolean applied = row.getBool("[applied]");
		if (!applied) {
			throw new DataSetAlreadyExistsException();
		}
		return mapRowToDataSet(row);
	}


	public List<DataSet> getDataSets(String providerId, String thresholdDatasetId, int limit)
			throws ProviderNotExistsException {
		if (thresholdDatasetId == null) {
			thresholdDatasetId = "";
		}
		BoundStatement boundStatement = listDataSetsStatement.
				bind(providerId, thresholdDatasetId, limit);
		ResultSet rs = connectionProvider.getSession().execute(boundStatement);
		List<DataSet> dataSets = new ArrayList<>(limit);
		for (Row row : rs) {
			dataSets.add(mapRowToDataSet(row));
		}
		return dataSets;
	}


	public void deleteDataSet(String providerId, String dataSetId)
			throws ProviderNotExistsException, DataSetNotExistsException {
		// remove all assignments
		String providerDataSetId = createProviderDataSetId(providerId, dataSetId);
		BoundStatement boundStatement = removeAssignmentsStatement.bind(providerDataSetId);
		connectionProvider.getSession().execute(boundStatement);

		// remove dataset itself
		boundStatement = deleteDataSetStatement.bind(providerId, dataSetId);
		connectionProvider.getSession().execute(boundStatement);
	}


	private String createProviderDataSetId(String providerId, String dataSetId) {
		return providerId + "\n" + dataSetId;
	}


	private DataSet mapRowToDataSet(Row row) {
		DataSet ds = new DataSet();
		ds.setId(row.getString("dataset_id"));
		ds.setProviderId(row.getString("provider_id"));
		ds.setDescription(row.getString("description"));
		return ds;
	}


	private Representation mapRowToRepresentationStub(String providerId, Row row) {
		Representation representation = new Representation();
		representation.setDataProvider(providerId);
		representation.setRecordId(row.getString("provider_id"));
		representation.setSchema(row.getString("schema_id"));
		representation.setVersion(row.getString("version"));
		return representation;
	}
	
}
