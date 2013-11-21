package eu.europeana.cloud.service.mcs.persistent;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

	@Autowired
	private CassandraConnectionProvider connectionProvider;

	private PreparedStatement getLatestRepresentationVersionStatement;

	private PreparedStatement insertNewLatestVersionStatement;

	private PreparedStatement updateLatestVersionStatement;

	private PreparedStatement insertRepresentationStatement;

	private PreparedStatement deleteRepresentationBatch;

	private PreparedStatement getRepresentationVersionStatement;

	private PreparedStatement listRepresentationVersionsStatement;

	private PreparedStatement persistRepresentationStatement;

	private PreparedStatement insertFileStatement;

	private PreparedStatement removeFileForNameStatement;

	private PreparedStatement removeFileStatement;

	private PreparedStatement getFilesStatement;

	private PreparedStatement getAllRepresentationsForRecord;


	@PostConstruct
	private void prepareStatements() {
		Session s = connectionProvider.getSession();
		getLatestRepresentationVersionStatement = s.prepare(
				"SELECT cloud_id, schema_id, version, provider_id, persistent FROM representations_latest_versions WHERE cloud_id = ? AND schema_id = ?;");
		insertNewLatestVersionStatement = s.prepare(
				"INSERT INTO representations_latest_versions (cloud_id, schema_id, version, provider_id, persistent) VALUES (?,?,?,?,?) IF NOT EXISTS;");
		updateLatestVersionStatement = s.prepare(
				"UPDATE representations_latest_versions SET version = ?, provider_id = ?, persistent = ? WHERE cloud_id=? AND schema_id=? IF version = ?;");
		insertRepresentationStatement = s.prepare(
				"INSERT INTO representations (cloud_id, schema_id, version, provider_id, persistent, creation_date) VALUES (?,?,?,?,?,?) IF NOT EXISTS;");
		deleteRepresentationBatch = s.prepare(
				"DELETE FROM representations WHERE cloud_id = ? AND schema_id = ? AND version = ?;");
		getRepresentationVersionStatement = s.prepare(
				"SELECT cloud_id, schema_id, version, provider_id, persistent FROM representations WHERE cloud_id = ? AND schema_id = ? AND version = ?;");
		listRepresentationVersionsStatement = s.prepare(
				"SELECT cloud_id, schema_id, version, provider_id, persistent FROM representations WHERE cloud_id = ? AND schema_id = ?;");
		persistRepresentationStatement = s.prepare(
				"UPDATE representations SET persistent = TRUE WHERE cloud_id = ? AND schema_id=? AND version = ? IF persistent = FALSE;");
		insertFileStatement = s.prepare(
				"INSERT INTO files(representation_id,file_name,mime_type,content_md5,content_length,last_modification_date) VALUES (?,?,?,?,?,?);");
		removeFileForNameStatement = s.prepare(
				"DELETE FROM files WHERE representation_id = ? AND file_name = ?;");
		removeFileStatement = s.prepare(
				"DELETE FROM files WHERE representation_id = ?;");
		getFilesStatement = s.prepare(
				"SELECT file_name, mime_type, content_md5, content_length, last_modification_date FROM files WHERE representation_id = ?;");
		getAllRepresentationsForRecord = s.prepare(
				"SELECT cloud_id, schema_id, version, provider_id, persistent FROM representations WHERE cloud_id = ?;");

	}


	public Record getRecord(String cloudId)
			throws RecordNotExistsException {
		ResultSet rs = connectionProvider.getSession().execute(removeFileStatement.bind(cloudId));
		Map<String, Representation> schemaToLatestPersistentRepresentation = new HashMap<>();
		for (Row row : rs) {
			Representation rep = mapToRepresentation(row);
			if (!rep.isPersistent()) {
				continue;
			}
			Representation prevRepresentationVersionForSchema = schemaToLatestPersistentRepresentation.get(rep.
					getSchema());
			if (prevRepresentationVersionForSchema == null) {
				schemaToLatestPersistentRepresentation.put(rep.getSchema(), rep);
			} else {
				int previousVersion = Integer.parseInt(prevRepresentationVersionForSchema.getVersion());
				int thisVersion = Integer.parseInt(rep.getVersion());
				if (thisVersion > previousVersion) {
					schemaToLatestPersistentRepresentation.put(rep.getVersion(), rep);
				}
			}
		}
		List<Representation> representations = new ArrayList<>(schemaToLatestPersistentRepresentation.values());
		return new Record(cloudId, representations);
	}


	public void deleteRecord(String cloudId)
			throws RecordNotExistsException {
		// remove from files
		// remove from representations_latest_versions
		// remove from representations
		throw new UnsupportedOperationException("Not implemented");
	}


	public List<Representation> findRepresentations(String providerId, String schema, String thresholdRecordId, int limit) {
		throw new UnsupportedOperationException("Not implemented");
	}


	public void deleteRepresentation(String cloudId, String schema)
			throws RecordNotExistsException, RepresentationNotExistsException {
		throw new UnsupportedOperationException("Not implemented");
	}


	public void deleteRepresentation(String cloudId, String schema, String version)
			throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException {
		Representation rep = getRepresentation(cloudId, schema, version);
		if (rep.isPersistent()) {
			throw new CannotModifyPersistentRepresentationException();
		}
		String representationKey = generateRepresentationKey(cloudId, schema, version);
		connectionProvider.getSession().execute(removeFileStatement.bind(representationKey));
		connectionProvider.getSession().execute(deleteRepresentationBatch.bind(cloudId, schema, version));
	}


	private Representation getLatestRepresentationVersion(String cloudId, String schema) {
		BoundStatement boundStatement = getLatestRepresentationVersionStatement.bind(cloudId, schema);
		ResultSet rs = connectionProvider.getSession().execute(boundStatement);
		Row row = rs.one();
		if (row == null) {
			return null;
		} else {
			return mapToRepresentation(row);
		}
	}


	public Representation getLatestPersistentRepresentation(String cloudId, String schema) {
		// TODO: This method might be more efficient...
		List<Representation> allRepresentations = this.listRepresentationVersions(cloudId, schema);
		// filter out temporary representations
		Representation latestRepresentation = null;
		int latestRepresentationVersion = 0;
		for (Representation r : allRepresentations) {
			if (r.isPersistent()) {
				int version = Integer.valueOf(r.getVersion());
				if (version > latestRepresentationVersion) {
					latestRepresentation = r;
					latestRepresentationVersion = version;
				}
			}
		}
		return latestRepresentation;
	}


	private Representation mapToRepresentation(Row row) {
		Representation representation = new Representation();
		representation.setDataProvider(row.getString("provider_id"));
		representation.setRecordId(row.getString("cloud_id"));
		representation.setSchema(row.getString("schema_id"));
		representation.setVersion(row.getString("version"));
		representation.setPersistent(row.getBool("persistent"));
		return representation;
	}


	private File mapToFile(Row row) {
		File f = new File();
		f.setContentLength(row.getLong("content_length"));
		// TODO: date formatting 
		f.setDate(row.getDate("last_modification_date").toString());
		f.setFileName("file_name");
		f.setMd5(row.getString("content_md5"));
		f.setMimeType(row.getString("mime_type"));
		return f;
	}


	public Representation createRepresentation(String cloudId, String schema, String providerId) {
		Date now = new Date();
		// get latest version
		Representation latestRepresentation = getLatestRepresentationVersion(cloudId, schema);
		// version number for new representation version 
		String versionNumber = generateNewVersionNumber(latestRepresentation, false);

		BoundStatement boundStatement;
		if (latestRepresentation == null) {
			// if this is first version - insert new value to latest_version if nobody inserted value meanwhile
			boundStatement = insertNewLatestVersionStatement.bind(cloudId, schema, versionNumber, providerId, false);
		} else {
			// if this is not first version - update row in table if it hasn't changed meanwhile
			String previousVersion = latestRepresentation.getVersion();
			boundStatement = updateLatestVersionStatement.
					bind(versionNumber, providerId, false, cloudId, schema, previousVersion);
		}
		Row row = connectionProvider.getSession().execute(boundStatement).one();
		boolean applied = row.getBool("[applied]");
		if (!applied) {
			// not applied - that means that someone must have inserted new version meanwhile, we must repeat the procedure!
			log.info("Transaction failed - could not insert new version number ({}) because it is not valid anymore.",
					versionNumber);
			return createRepresentation(cloudId, schema, providerId);
		}
		// insert representation into representation table.
		boundStatement = insertRepresentationStatement.bind(cloudId, schema, versionNumber, providerId, false, now);
		connectionProvider.getSession().execute(boundStatement).one();
		return new Representation(cloudId, schema, versionNumber, null, null, providerId, new ArrayList<File>(0), false);
	}


	/**
	 * Generates new version number for persistent or temporary representation. Persistent versions have numbers: \d+.
	 * Temp versions have numbers: \d+[.]PRE-\d+
	 *
	 * @param latestRepresentation latest existing representation (persistent or not). May be null if there is no
	 * previous version.
	 * @param persistent true if we are going to generate new persistent version. False if the generated version is
	 * temporary.
	 * @return new version number.
	 */
	private String generateNewVersionNumber(Representation latestRepresentation, boolean persistent) {
		// now: versions have just sequential numbers, no distinction between temp and persistent
		if (latestRepresentation == null) {
			return "1";
		} else {
			return Integer.toString(1 + Integer.parseInt(latestRepresentation.getVersion()));
		}

//		if (latestRepresentation == null) {
//			if (persistent) {
//				return "1";
//			} else {
//				return "1.PRE-1";
//			}
//		}
//
//		boolean latestIsPersistent = latestRepresentation.isPersistent();
//		if (latestIsPersistent && persistent) {
//			return Integer.toString(Integer.parseInt(latestRepresentation.getVersion()) + 1);
//		} else if (latestIsPersistent && !persistent) {
//			return Integer.toString(Integer.parseInt(latestRepresentation.getVersion()) + 1) + ".PRE-1";
//		} else if (persistent) {
//			return latestRepresentation.getVersion().substring(0, latestRepresentation.getVersion().indexOf("."));
//		} else {
//			String[] parts = latestRepresentation.getVersion().split("-");
//			return parts[0] + "-" + (Integer.parseInt(parts[1]) + 1);
//		}
	}


	public Representation getRepresentation(String cloudId, String schema, String version) {
		if (cloudId == null || schema == null || version == null) {
			throw new IllegalArgumentException("Parameters cannot be null");
		}
		BoundStatement boundStatement = getRepresentationVersionStatement.bind(cloudId, schema, version);
		ResultSet rs = connectionProvider.getSession().execute(boundStatement);
		Row row = rs.one();
		if (row == null) {
			throw new RepresentationNotExistsException();
		} else {
			return mapToRepresentation(row);
		}
	}


	public List<File> getFilesForRepresentation(String cloudId, String schema, String version) {
		String representationKey = generateRepresentationKey(cloudId, schema, version);
		BoundStatement boundStatement = getFilesStatement.bind(representationKey);
		ResultSet rs = connectionProvider.getSession().execute(boundStatement);
		List<File> result = new ArrayList<>(rs.getAvailableWithoutFetching());
		for (Row row : rs) {
			result.add(mapToFile(row));
		}
		return result;
	}


	public Representation persistRepresentation(String cloudId, String schema, String version)
			throws RepresentationNotExistsException, CannotModifyPersistentRepresentationException {
//		throw new UnsupportedOperationException("Not implemented");
		// check if representation has at least one file - otherwhise, such representation has no meaning.
		// first - check if such representation exists and is not persistent
		Representation representation = getRepresentation(cloudId, schema, version);
		if (representation == null) {
			throw new RepresentationNotExistsException();
		} else if (representation.isPersistent()) {
			throw new CannotModifyPersistentRepresentationException("Already persistent");
		}

		BoundStatement boundStatement = persistRepresentationStatement.bind(cloudId, schema, version);
		ResultSet rs = connectionProvider.getSession().execute(boundStatement);
		Row row = rs.one();
		boolean applied = row.getBool("[applied]");
		if (applied) {
			return new Representation(cloudId, schema, version, null, null, representation.
					getDataProvider(), new ArrayList<File>(), true);
		} else {
			boolean representationExisted = rs.getColumnDefinitions().contains("persistent");
			if (representationExisted) {
				boolean wasAlreadyPersistent = row.getBool("persistent");
				if (wasAlreadyPersistent) {
					throw new CannotModifyPersistentRepresentationException();
				} else {
					throw new IllegalStateException("Could not persist row but it was not persistent before");
				}
			} else {
				throw new RepresentationNotExistsException("Representation does not exist anymore");
			}
		}

		// generate new number for persistent representation
//		Representation latestRepresentation = getLatestRepresentationVersion(cloudId, schema);
//		if (latestRepresentation == null) {
//			throw new RepresentationNotExistsException();
//		}
//		String persistentVersionNumber = generateNewVersionNumber(latestRepresentation, true);
//
//		// now update latest versions table - because this will be our latest version
//		BoundStatement boundStatement = updateLatestVersionStatement.
//				bind(persistentVersionNumber, latestRepresentation.getDataProvider(), true, cloudId, schema, latestRepresentation.
//						getVersion());
//		Row row = connectionProvider.getSession().execute(boundStatement).one();
//		boolean applied = row.getBool("[applied]");
//		if (!applied) {
//			// something must have changed meanwhile... Redo whole procedure
//			log.
//					info("Transaction failed - could not persist to new version number ({}) because it is not valid anymore.",
//							persistentVersionNumber);
//			return persistRepresentation(cloudId, schema, version);
//		}
//
//		// update in representations table
//		boundStatement = persistRepresentationStatement.bind(persistentVersionNumber, cloudId, schema, version);
//		ResultSet rs = connectionProvider.getSession().execute(boundStatement);
//		row = rs.one();
		// check if change has been applied
	}


	public List<Representation> listRepresentationVersions(String cloudId, String schema)
			throws RecordNotExistsException, RepresentationNotExistsException {
		BoundStatement boundStatement = listRepresentationVersionsStatement.bind(cloudId, schema);
		ResultSet rs = connectionProvider.getSession().execute(boundStatement);
		List<Representation> result = new ArrayList<>(rs.getAvailableWithoutFetching());
		for (Row row : rs) {
			result.add(mapToRepresentation(row));
		}
		return result;
	}


	public void addOrReplaceFileInRepresentation(String cloudId, String schema, String version, File file) {
		Date now = new Date();
		String representationKey = generateRepresentationKey(cloudId, schema, version);
		BoundStatement boundStatement = insertFileStatement.bind(representationKey, file.getFileName(),
				file.getMimeType(), file.getMd5(), file.getContentLength(), now);
		connectionProvider.getSession().execute(boundStatement);
	}


	public void removeFileFromRepresentation(String cloudId, String schema, String version, String fileName) {
		String representationKey = generateRepresentationKey(cloudId, schema, version);
		BoundStatement boundStatement = removeFileForNameStatement.bind(representationKey, fileName);
		connectionProvider.getSession().execute(boundStatement);
	}


	private String generateRepresentationKey(String cloudId, String schema, String version) {
		return cloudId + "\n" + schema + "\n" + version;
	}
}
