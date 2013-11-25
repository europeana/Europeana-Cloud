package eu.europeana.cloud.service.mcs.persistent;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.gson.Gson;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

/**
 *
 * @author sielski
 */
@Repository
public class CassandraRecordDAO {

	private final static Logger log = LoggerFactory.getLogger(CassandraConnectionProvider.class);

	private final Gson gson = new Gson();

	@Autowired
	private CassandraConnectionProvider connectionProvider;

	private PreparedStatement insertRepresentationStatement;

	private PreparedStatement deleteRepresentationVersionStatement;

	private PreparedStatement deleteRecordStatement;

	private PreparedStatement deleteRepresentationStatement;

	private PreparedStatement getRepresentationVersionStatement;

	private PreparedStatement listRepresentationVersionsStatement;

	private PreparedStatement persistRepresentationStatement;

	private PreparedStatement insertFileStatement;

	private PreparedStatement removeFileStatement;

	private PreparedStatement getFilesStatement;

	private PreparedStatement getAllRepresentationsForRecord;


	@PostConstruct
	private void prepareStatements() {
		Session s = connectionProvider.getSession();

		insertRepresentationStatement = s.prepare(
				"INSERT INTO representations (cloud_id, schema_id, version, provider_id, persistent, creation_date) VALUES (?,?,?,?,?,?);");
		getRepresentationVersionStatement = s.prepare(
				"SELECT cloud_id, schema_id, version, provider_id, persistent, files FROM representations WHERE cloud_id = ? AND schema_id = ? AND version = ?;");
		listRepresentationVersionsStatement = s.prepare(
				"SELECT cloud_id, schema_id, version, provider_id, persistent FROM representations WHERE cloud_id = ? AND schema_id = ? ORDER BY schema_id DESC, version DESC;");
		persistRepresentationStatement = s.prepare(
				"UPDATE representations SET persistent = TRUE WHERE cloud_id = ? AND schema_id=? AND version = ?;");
		insertFileStatement = s.prepare(
				"UPDATE representations SET files[?] = ? WHERE cloud_id = ? AND schema_id = ? AND version = ?;");
		removeFileStatement = s.prepare(
				"DELETE files[?] FROM representations WHERE cloud_id = ? AND schema_id = ? AND version = ?;");
		getFilesStatement = s.prepare(
				"SELECT files FROM representations WHERE cloud_id = ? AND schema_id = ? AND version = ?;");
		getAllRepresentationsForRecord = s.prepare(
				"SELECT cloud_id, schema_id, version, provider_id, persistent FROM representations WHERE cloud_id = ? ORDER BY schema_id DESC, version DESC;");
		deleteRecordStatement = s.prepare(
				"BEGIN BATCH "
				+ "DELETE FROM representations WHERE cloud_id = ? "
				+ "DELETE FROM data_set_assignments WHERE cloud_id = ? "
				+ "APPLY BATCH;");
		deleteRepresentationStatement = s.prepare(
				"BEGIN BATCH "
				+ "DELETE FROM representations WHERE cloud_id = ? AND schema_id = ? "
				+ "DELETE FROM data_set_assignments WHERE cloud_id = ? AND schema_id = ? "
				+ "APPLY BATCH;");
		deleteRepresentationVersionStatement = s.prepare(
				"BEGIN BATCH "
				+ "DELETE FROM representations WHERE cloud_id = ? AND schema_id = ? AND version = ? "
				+ "DELETE FROM data_set_assignments WHERE cloud_id = ? AND schema_id = ? "
				+ "APPLY BATCH;");
	}


	/**
	 * Returns a record containing latest persistent representation in each schema. If there is no persistent
	 * representation in some schema, it would not be returned.
	 *
	 * @param cloudId
	 * @return
	 */
	public Record getRecord(String cloudId) {
		ResultSet rs = connectionProvider.getSession().execute(getAllRepresentationsForRecord.bind(cloudId));
		List<Representation> representations = new ArrayList<>();
		String prevSchema = null;
		for (Row row : rs) {
			Representation rep = mapToRepresentation(row);
			if (rep.isPersistent() && !rep.getSchema().equals(prevSchema)) {
				representations.add(rep);
				prevSchema = rep.getSchema();
			}
		}
		return new Record(cloudId, representations);
	}


	public void deleteRecord(String cloudId) {
		connectionProvider.getSession().execute(deleteRecordStatement.bind(cloudId, cloudId));
	}


	public List<Representation> findRepresentations(String providerId, String schema, String thresholdRecordId, int limit) {
		throw new UnsupportedOperationException("Not implemented");
	}


	public void deleteRepresentation(String cloudId, String schema) {
		connectionProvider.getSession().execute(deleteRepresentationStatement.bind(cloudId, schema, cloudId, schema));
	}


	public void deleteRepresentation(String cloudId, String schema, String version) {
		connectionProvider.getSession().execute(deleteRepresentationVersionStatement.
				bind(cloudId, schema, UUID.fromString(version), cloudId, schema));
	}


	public Representation getLatestPersistentRepresentation(String cloudId, String schema) {
		List<Representation> allRepresentations = this.listRepresentationVersions(cloudId, schema);

		for (Representation r : allRepresentations) {
			if (r.isPersistent()) {
				r.setFiles(getFilesForRepresentation(cloudId, schema, r.getVersion()));
				return r;
			}
		}

		return null;
	}


	public Representation createRepresentation(String cloudId, String schema, String providerId) {
		if (cloudId == null || schema == null || providerId == null) {
			throw new IllegalArgumentException("Parameters cannot be null");
		}
		Date now = new Date();
		UUID version = getTimeUUID();

		// insert representation into representation table.
		BoundStatement boundStatement = insertRepresentationStatement.
				bind(cloudId, schema, version, providerId, false, now);
		connectionProvider.getSession().execute(boundStatement);
		return new Representation(cloudId, schema, version.toString(), null, null, providerId, new ArrayList<File>(0), false);
	}


	public Representation getRepresentation(String cloudId, String schema, String version) {
		if (cloudId == null || schema == null || version == null) {
			throw new IllegalArgumentException("Parameters cannot be null");
		}
		BoundStatement boundStatement = getRepresentationVersionStatement.
				bind(cloudId, schema, UUID.fromString(version));
		ResultSet rs = connectionProvider.getSession().execute(boundStatement);
		Row row = rs.one();
		if (row == null) {
			throw new RepresentationNotExistsException();
		} else {
			Representation rep = mapToRepresentation(row);
			rep.setFiles(deserializeFiles(row.getMap("files", String.class, String.class)));
			return rep;
		}
	}


	/**
	 * Returns files for representation. If threre is no such representation - will return null. If representation does
	 * not contain any files - will return empty list.
	 *
	 * @param cloudId
	 * @param schema
	 * @param version
	 * @return
	 */
	public List<File> getFilesForRepresentation(String cloudId, String schema, String version) {
		BoundStatement boundStatement = getFilesStatement.bind(cloudId, schema, UUID.fromString(version));
		Row row = connectionProvider.getSession().execute(boundStatement).one();
		if (row == null) {
			return null;
		} else {
			Map<String, String> fileNameToFile = row.getMap("files", String.class, String.class);
			return deserializeFiles(fileNameToFile);

		}
	}


	public void persistRepresentation(String cloudId, String schema, String version) {
		BoundStatement boundStatement = persistRepresentationStatement.bind(cloudId, schema, UUID.fromString(version));
		connectionProvider.getSession().execute(boundStatement);
	}


	/**
	 * Returns all versions of representation (persistent or not), sorted from the most recent to the oldes. If no
	 * representation exist - will return empty list.
	 *
	 * @param cloudId
	 * @param schema
	 * @return
	 */
	public List<Representation> listRepresentationVersions(String cloudId, String schema) {
		BoundStatement boundStatement = listRepresentationVersionsStatement.bind(cloudId, schema);
		ResultSet rs = connectionProvider.getSession().execute(boundStatement);
		List<Representation> result = new ArrayList<>(rs.getAvailableWithoutFetching());
		for (Row row : rs) {
			result.add(mapToRepresentation(row));
		}
		return result;
	}


	public void addOrReplaceFileInRepresentation(String cloudId, String schema, String version, File file) {
		BoundStatement boundStatement = insertFileStatement.
				bind(file.getFileName(), serializeFile(file), cloudId, schema, UUID.fromString(version));
		connectionProvider.getSession().execute(boundStatement);
	}


	public void removeFileFromRepresentation(String cloudId, String schema, String version, String fileName) {
		BoundStatement boundStatement = removeFileStatement.bind(fileName, cloudId, schema, UUID.fromString(version));
		connectionProvider.getSession().execute(boundStatement);
	}


	private Representation mapToRepresentation(Row row) {
		Representation representation = new Representation();
		representation.setDataProvider(row.getString("provider_id"));
		representation.setRecordId(row.getString("cloud_id"));
		representation.setSchema(row.getString("schema_id"));
		representation.setVersion(row.getUUID("version").toString());
		representation.setPersistent(row.getBool("persistent"));

		return representation;
	}


	private List<File> deserializeFiles(Map<String, String> fileNameToFile) {
		if (fileNameToFile == null) {
			return new ArrayList<>(0);
		}
		List<File> files = new ArrayList<>(fileNameToFile.size());
		for (String fileJSON : fileNameToFile.values()) {
			files.add(gson.fromJson(fileJSON, File.class));
		}
		return files;
	}


	private String serializeFile(File f) {
		f.setContentUri(null);
		return gson.toJson(f);
	}
	
	private static UUID getTimeUUID() {
		return UUID.fromString(new com.eaio.uuid.UUID().toString());
	}
}
