
package eu.europeana.cloud.service.mcs.persistent;

import com.datastax.driver.core.PreparedStatement;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.response.ResultSlice;
import eu.europeana.cloud.service.mcs.exception.CannotModifyPersistentRepresentationException;
import eu.europeana.cloud.service.mcs.exception.FileAlreadyExistsException;
import eu.europeana.cloud.service.mcs.exception.FileNotExistsException;
import eu.europeana.cloud.service.mcs.exception.ProviderNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RecordNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.VersionNotExistsException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

/**
 *
 * @author sielski
 */
@Repository
public class CassandraRecordDAO {
	
	@Autowired
	private CassandraConnectionProvider connectionProvider;

	private PreparedStatement getLatestRepresentationVersionStatement;
	private PreparedStatement getLatestPersistentRepresentationVersionStatement;

		@PostConstruct
	private void prepareStatements() {
		getLatestRepresentationVersionStatement = connectionProvider.getSession().prepare(
				"SELECT version FROM representations_latest_versions WHERE cloud_id = ? AND schema_id = ?;");
		getLatestPersistentRepresentationVersionStatement = connectionProvider.getSession().prepare(
				"SELECT version FROM representations_latest_versions WHERE cloud_id = ? AND schema_id = ? AND persistent = TRUE;");
	}

	
	public Record getRecord(String globalId)
			throws RecordNotExistsException {
		throw new UnsupportedOperationException("Not implemented");

	}


	public void deleteRecord(String globalId)
			throws RecordNotExistsException {
		// remove from files
		// remove from representations_latest_versions
		// remove from representations

	}


	public List<Representation> findRepresentations(String providerId, String schema) {
		throw new UnsupportedOperationException("Not implemented");

	}


	public void deleteRepresentation(String globalId, String schema)
			throws RecordNotExistsException, RepresentationNotExistsException {
		throw new UnsupportedOperationException("Not implemented");

	}


	public Representation createRepresentation(String globalId, String schema, String providerId) {
		throw new UnsupportedOperationException("Not implemented");
		// get latest version
		// construct new version number
		// insert into latest_versions if previously fetched value is still valid 
		// if not valid - repeat previous steps until new version number is guaranteed to be unique
		// isert representation into representation table.

	}

// persistentVersion: \d+
	// tempVersion: \d+[.]PRE-\d+

	private String generateNewVersionNumber(List<Representation> versions, boolean persistent) {
		if (versions.isEmpty()) {
			if (persistent) {
				return "1";
			} else {
				return "1.PRE-1";
			}
		}
		Representation latestRepresentation = versions.get(versions.size() - 1);
		boolean latestIsPersistent = latestRepresentation.isPersistent();
		if (latestIsPersistent && persistent) {
			return Integer.toString(Integer.parseInt(latestRepresentation.getVersion()) + 1);
		} else if (latestIsPersistent && !persistent) {
			return Integer.toString(Integer.parseInt(latestRepresentation.getVersion()) + 1) + ".PRE-1";
		} else if (persistent) {
			return latestRepresentation.getVersion().substring(0, latestRepresentation.getVersion().indexOf("."));
		} else {
			String[] parts = latestRepresentation.getVersion().split("-");
			return parts[0] + "-" + (Integer.parseInt(parts[1]) + 1);
		}
	}


	public Representation getRepresentation(String globalId, String schema, String version)
			throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException {
		throw new UnsupportedOperationException("Not implemented");

	}


	public void deleteRepresentation(String globalId, String schema, String version)
			throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException {
		throw new UnsupportedOperationException("Not implemented");

	}


	public Representation persistRepresentation(String globalId, String schema, String version)
			throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException, CannotModifyPersistentRepresentationException {
		throw new UnsupportedOperationException("Not implemented");

	}


	public List<Representation> listRepresentationVersions(String globalId, String schema)
			throws RecordNotExistsException, RepresentationNotExistsException {
		throw new UnsupportedOperationException("Not implemented");

	}


	public Representation addOrReplaceFileInRepresentation(String globalId, String schema, String version, File file)
			throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException, FileAlreadyExistsException {
		throw new UnsupportedOperationException("Not implemented");

	}


	public Representation removeFileFromRepresentation(String globalId, String schema, String version, String fileName)
			throws RecordNotExistsException, RepresentationNotExistsException, VersionNotExistsException, FileNotExistsException {
		throw new UnsupportedOperationException("Not implemented");

	}
}
