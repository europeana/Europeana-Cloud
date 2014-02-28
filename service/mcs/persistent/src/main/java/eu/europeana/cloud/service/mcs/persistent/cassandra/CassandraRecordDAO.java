package eu.europeana.cloud.service.mcs.persistent.cassandra;

import eu.europeana.cloud.service.mcs.persistent.util.QueryTracer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.google.gson.Gson;

import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;

/**
 * Repository for records, their representations and versions. Uses Cassandra as storage.
 */
@Repository
public class CassandraRecordDAO {

    // json serializer/deserializer
    private final Gson gson = new Gson();

    @Autowired
    private CassandraConnectionProvider connectionProvider;

    private PreparedStatement insertRepresentationStatement;

    private PreparedStatement deleteRepresentationVersionStatement;

    private PreparedStatement deleteRecordStatement;

    private PreparedStatement deleteRepresentationStatement;

    private PreparedStatement getRepresentationVersionStatement;

    private PreparedStatement listRepresentationVersionsStatement;

    private PreparedStatement listRepresentationVersionsAllSchemasStatement;

    private PreparedStatement persistRepresentationStatement;

    private PreparedStatement insertFileStatement;

    private PreparedStatement removeFileStatement;

    private PreparedStatement getFilesStatement;

    private PreparedStatement getAllRepresentationsForRecordStatement;

    private PreparedStatement singleRecordIdForProviderStatement;


    @PostConstruct
    private void prepareStatements() {
        Session s = connectionProvider.getSession();

        insertRepresentationStatement = s
                .prepare("INSERT INTO representation_versions (cloud_id, schema_id, version_id, provider_id, persistent, creation_date) VALUES (?,?,?,?,?,?);");
        insertRepresentationStatement.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        getRepresentationVersionStatement = s
                .prepare("SELECT cloud_id, schema_id, version_id, provider_id, persistent, creation_date, files FROM representation_versions WHERE cloud_id = ? AND schema_id = ? AND version_id = ?;");
        getRepresentationVersionStatement.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        listRepresentationVersionsStatement = s
                .prepare("SELECT cloud_id, schema_id, version_id, provider_id, persistent, creation_date FROM representation_versions WHERE cloud_id = ? AND schema_id = ? ORDER BY schema_id DESC, version_id DESC;");
        listRepresentationVersionsStatement.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        listRepresentationVersionsAllSchemasStatement = s
                .prepare("SELECT cloud_id, schema_id, version_id, provider_id, persistent, creation_date FROM representation_versions WHERE cloud_id = ?;");
        listRepresentationVersionsAllSchemasStatement.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        persistRepresentationStatement = s
                .prepare("UPDATE representation_versions SET persistent = TRUE, creation_date = ? WHERE cloud_id = ? AND schema_id=? AND version_id = ?;");
        persistRepresentationStatement.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        insertFileStatement = s
                .prepare("UPDATE representation_versions SET files[?] = ? WHERE cloud_id = ? AND schema_id = ? AND version_id = ?;");
        insertFileStatement.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        removeFileStatement = s
                .prepare("DELETE files[?] FROM representation_versions WHERE cloud_id = ? AND schema_id = ? AND version_id = ?;");
        removeFileStatement.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        getFilesStatement = s
                .prepare("SELECT files FROM representation_versions WHERE cloud_id = ? AND schema_id = ? AND version_id = ?;");
        getFilesStatement.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        getAllRepresentationsForRecordStatement = s
                .prepare("SELECT cloud_id, schema_id, version_id, provider_id, persistent, creation_date FROM representation_versions WHERE cloud_id = ? ORDER BY schema_id DESC, version_id DESC;");
        getAllRepresentationsForRecordStatement.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        deleteRecordStatement = s.prepare("BEGIN BATCH " + "DELETE FROM representation_versions WHERE cloud_id = ? "
                + "DELETE FROM data_set_assignments WHERE cloud_id = ? " + "APPLY BATCH;");
        deleteRecordStatement.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        deleteRepresentationStatement = s.prepare("BEGIN BATCH "
                + "DELETE FROM representation_versions WHERE cloud_id = ? AND schema_id = ? "
                + "DELETE FROM data_set_assignments WHERE cloud_id = ? AND schema_id = ? " + "APPLY BATCH;");
        deleteRepresentationStatement.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        deleteRepresentationVersionStatement = s.prepare("BEGIN BATCH "
                + "DELETE FROM representation_versions WHERE cloud_id = ? AND schema_id = ? AND version_id = ? "
                + "DELETE FROM data_set_assignments WHERE cloud_id = ? AND schema_id = ? " + "APPLY BATCH;");
        deleteRepresentationVersionStatement.setConsistencyLevel(connectionProvider.getConsistencyLevel());

        singleRecordIdForProviderStatement = s
                .prepare("SELECT cloud_id FROM representation_versions WHERE provider_id = ? LIMIT 1;");
        singleRecordIdForProviderStatement.setConsistencyLevel(connectionProvider.getConsistencyLevel());
    }


    /**
     * Returns a record containing latest persistent representation in each schema. If there is no persistent
     * representation in some schema, it would not be returned.
     * 
     * @param cloudId
     * @return
     */
    public Record getRecord(String cloudId)
            throws NoHostAvailableException, QueryExecutionException {
        final BoundStatement boundStatement = getAllRepresentationsForRecordStatement.bind(cloudId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
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


    /**
     * Deletes record with all representations and all versions. If such record doesn't exist - nothing happens.
     * 
     * @param cloudId
     *            indentifier of record to be deleted.
     */
    public void deleteRecord(String cloudId)
            throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = deleteRecordStatement.bind(cloudId, cloudId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }


    /**
     * Deletes representation with all versions. If such represenation doesn't exist - nothing happens.
     * 
     * @param cloudId
     *            identifier of record
     * @param schema
     *            schema of representation to be deleted.
     */
    public void deleteRepresentation(String cloudId, String schema)
            throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = deleteRepresentationStatement.bind(cloudId, schema, cloudId, schema);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }


    /**
     * Deletes record's representation in specified version. If such version doesn't exist - nothing happens. This
     * method doesn't check any constraints e.g. can delete persistent version.
     * 
     * @param cloudId
     *            identifier of record
     * @param schema
     *            schema of representation
     * @param version
     *            version of representation
     */
    public void deleteRepresentation(String cloudId, String schema, String version)
            throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = deleteRepresentationVersionStatement.bind(cloudId, schema,
            UUID.fromString(version), cloudId, schema);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }


    /**
     * Returns latest persistent version of record's representation. If there is no such representation or no version of
     * this representation is persistent - will return null.
     * 
     * @param cloudId
     *            identifier of record
     * @param schema
     *            schema of representation
     * @return latest persistent version of a representation or null if such doesn't exist.
     */
    public Representation getLatestPersistentRepresentation(String cloudId, String schema) {
        List<Representation> allRepresentations;
        try {
            allRepresentations = this.listRepresentationVersions(cloudId, schema);
            for (Representation r : allRepresentations) {
                if (r.isPersistent()) {
                    r.setFiles(getFilesForRepresentation(cloudId, schema, r.getVersion()));
                    return r;
                }
            }
        } catch (RepresentationNotExistsException ex) { //don't rethrow, just return null
            return null;
        }
        return null;
    }


    /**
     * Creates new temporary version for a specific record's representation.
     * 
     * @param cloudId
     *            identifier of record
     * @param schema
     *            schema of representation
     * @param providerId
     *            representation version provider
     * @param creationTime
     *            creation date
     * @return
     */
    public Representation createRepresentation(String cloudId, String schema, String providerId, Date creationTime)
            throws NoHostAvailableException, QueryExecutionException {
        if (cloudId == null || schema == null || providerId == null) {
            throw new IllegalArgumentException("Parameters cannot be null");
        }
        UUID version = getTimeUUID();

        // insert representation into representation table.
        BoundStatement boundStatement = insertRepresentationStatement.bind(cloudId, schema, version, providerId, false,
            creationTime);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        return new Representation(cloudId, schema, version.toString(), null, null, providerId, new ArrayList<File>(0),
                false, creationTime);
    }


    /**
     * Returns a representation of a record in specified schema and version.
     * 
     * @param cloudId
     *            id of the record
     * @param schema
     *            schema of the representation
     * @param version
     *            version of the representation.
     * @return representation.
     * @throws QueryExecutionException
     *             if error occured while executing a query.
     * @throws NoHostAvailableException
     *             if no Cassandra host are available.
     */
    public Representation getRepresentation(String cloudId, String schema, String version)
            throws NoHostAvailableException, QueryExecutionException {
        if (cloudId == null || schema == null || version == null) {
            throw new IllegalArgumentException("Parameters cannot be null");
        }
        BoundStatement boundStatement = getRepresentationVersionStatement.bind(cloudId, schema,
            UUID.fromString(version));
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);

        QueryTracer.logConsistencyLevel(boundStatement, rs);

        Row row = rs.one();
        if (row == null) {
            return null;
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
    public List<File> getFilesForRepresentation(String cloudId, String schema, String version)
            throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = getFilesStatement.bind(cloudId, schema, UUID.fromString(version));
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        Row row = rs.one();
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        if (row == null) {
            return null;
        } else {
            Map<String, String> fileNameToFile = row.getMap("files", String.class, String.class);
            return deserializeFiles(fileNameToFile);
        }
    }


    /**
     * Makes a certain temporary representation version a persistent one. Sets creation date.
     * 
     * @param cloudId
     *            id of the record
     * @param schema
     *            schema of the representation
     * @param version
     *            version of the representation
     * @param creationTime
     *            date of creation
     * @throws QueryExecutionException
     *             if error occured while executing a query.
     * @throws NoHostAvailableException
     *             if no Cassandra host are available.
     * 
     */
    public void persistRepresentation(String cloudId, String schema, String version, Date creationTime)
            throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = persistRepresentationStatement.bind(creationTime, cloudId, schema,
            UUID.fromString(version));
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }


    /**
     * Returns all versions of representation (persistent or not), sorted from the most recent to the oldes. If no
     * representation exist - will return empty list.
     * 
     * @param cloudId
     *            record id
     * @param schema
     *            schema id
     * @throws RepresentationNotExistsException
     *             when there is no representation matching requested parameters
     * @return
     */
    public List<Representation> listRepresentationVersions(String cloudId, String schema)
            throws NoHostAvailableException, QueryExecutionException, RepresentationNotExistsException {
        BoundStatement boundStatement = listRepresentationVersionsStatement.bind(cloudId, schema);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        List<Representation> result = new ArrayList<>(rs.getAvailableWithoutFetching());
        if (rs.isExhausted()) {
            throw new RepresentationNotExistsException();
        }
        for (Row row : rs) {
            result.add(mapToRepresentation(row));
        }
        return result;
    }


    /**
     * Returns all versions of all representations (persistent or not) for a cloud id.
     * 
     * @param cloudId
     *            record id
     * @return
     */
    public List<Representation> listRepresentationVersions(String cloudId)
            throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = listRepresentationVersionsAllSchemasStatement.bind(cloudId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        List<Representation> result = new ArrayList<>(rs.getAvailableWithoutFetching());
        for (Row row : rs) {
            result.add(mapToRepresentation(row));
        }
        return result;
    }


    /**
     * Adds or modifies given file to list of files of representation.
     * 
     * @param cloudId
     *            record if
     * @param schema
     *            schema id
     * @param version
     *            version id
     * @param file
     *            file
     * @throws QueryExecutionException
     *             if error occured while executing a query.
     * @throws NoHostAvailableException
     *             if no Cassandra host are available.
     */
    public void addOrReplaceFileInRepresentation(String cloudId, String schema, String version, File file)
            throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = insertFileStatement.bind(file.getFileName(), serializeFile(file), cloudId,
            schema, UUID.fromString(version));
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }


    /**
     * Removes file entry from list of files belonging to record representation.
     * 
     * @param cloudId
     *            record if
     * @param schema
     *            schema id
     * @param version
     *            version id
     * @param fileName
     *            name of file to be removed from representation
     * @throws QueryExecutionException
     *             if error occured while executing a query.
     * @throws NoHostAvailableException
     *             if no Cassandra host are available.
     */
    public void removeFileFromRepresentation(String cloudId, String schema, String version, String fileName)
            throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = removeFileStatement.bind(fileName, cloudId, schema, UUID.fromString(version));
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }


    /**
     * Checks if given provider has any representations.
     * 
     * @param providerId
     *            identifier of the provider
     * @return true if provider has representations, false otherwise
     * @throws QueryExecutionException
     *             if error occured while executing a query.
     * @throws NoHostAvailableException
     *             if no Cassandra host are available.
     */
    public boolean providerHasRepresentations(String providerId)
            throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = singleRecordIdForProviderStatement.bind(providerId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        Row row = rs.one();
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        return row != null;
    }


    private Representation mapToRepresentation(Row row) {
        Representation representation = new Representation();
        representation.setDataProvider(row.getString("provider_id"));
        representation.setRecordId(row.getString("cloud_id"));
        representation.setSchema(row.getString("schema_id"));
        representation.setVersion(row.getUUID("version_id").toString());
        representation.setPersistent(row.getBool("persistent"));
        representation.setCreationDate(row.getDate("creation_date"));
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
