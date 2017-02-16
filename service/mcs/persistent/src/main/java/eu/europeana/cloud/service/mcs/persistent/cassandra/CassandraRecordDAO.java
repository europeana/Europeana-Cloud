package eu.europeana.cloud.service.mcs.persistent.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.model.File;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.common.model.Representation;
import eu.europeana.cloud.common.model.Revision;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import eu.europeana.cloud.common.utils.RevisionUtils;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.exception.RevisionIsNotValidException;
import eu.europeana.cloud.service.mcs.persistent.util.QueryTracer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.util.*;

/**
 * Repository for records, their representations and versions. Uses Cassandra as
 * storage.
 */
@Repository
public class CassandraRecordDAO {

    // json serializer/deserializer
    private final Gson gson = new Gson();
    private final Gson revisionGson = new GsonBuilder()
            .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ").create();

    @Autowired
    @Qualifier("dbService")
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

    private PreparedStatement insertRevisionStatement;

    private PreparedStatement getRepresentationRevisionStatement;

    private PreparedStatement getLatestRepresentationRevisionStatement;

    private PreparedStatement removeFileStatement;

    private PreparedStatement getFilesStatement;

    private PreparedStatement getAllRepresentationsForRecordStatement;

    private PreparedStatement singleRecordIdForProviderStatement;

    private PreparedStatement insertRepresentationRevisionStatement;

    private PreparedStatement insertRepresentationRevisionFileStatement;

    private PreparedStatement deleteRepresentationRevisionStatement;

    @PostConstruct
    private void prepareStatements() {
        Session s = connectionProvider.getSession();

        insertRepresentationStatement = s
                .prepare("INSERT INTO representation_versions (cloud_id, schema_id, version_id, provider_id, persistent, creation_date) VALUES (?,?,?,?,?,?);");
        insertRepresentationStatement.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());

        getRepresentationVersionStatement = s
                .prepare("SELECT cloud_id, schema_id, version_id, provider_id, persistent, creation_date, files,revisions FROM representation_versions WHERE cloud_id = ? AND schema_id = ? AND version_id = ?;");
        getRepresentationVersionStatement
                .setConsistencyLevel(connectionProvider.getConsistencyLevel());

        listRepresentationVersionsStatement = s
                .prepare("SELECT cloud_id, schema_id, version_id, provider_id, persistent, creation_date, files,revisions FROM representation_versions WHERE cloud_id = ? AND schema_id = ? ORDER BY schema_id DESC, version_id DESC;");
        listRepresentationVersionsStatement
                .setConsistencyLevel(connectionProvider.getConsistencyLevel());

        listRepresentationVersionsAllSchemasStatement = s
                .prepare("SELECT cloud_id, schema_id, version_id, provider_id, persistent, creation_date, files,revisions FROM representation_versions WHERE cloud_id = ?;");
        listRepresentationVersionsAllSchemasStatement
                .setConsistencyLevel(connectionProvider.getConsistencyLevel());

        persistRepresentationStatement = s
                .prepare("UPDATE representation_versions SET persistent = TRUE, creation_date = ? WHERE cloud_id = ? AND schema_id=? AND version_id = ?;");
        persistRepresentationStatement.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());

        insertFileStatement = s
                .prepare("UPDATE representation_versions SET files[?] = ? WHERE cloud_id = ? AND schema_id = ? AND version_id = ?;");
        insertFileStatement.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());


        insertRevisionStatement = s
                .prepare("UPDATE representation_versions SET revisions[?] = ? WHERE cloud_id = ? AND schema_id = ? AND version_id = ?;");
        insertRevisionStatement.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());

        removeFileStatement = s
                .prepare("DELETE files[?] FROM representation_versions WHERE cloud_id = ? AND schema_id = ? AND version_id = ?;");
        removeFileStatement.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());

        getFilesStatement = s
                .prepare("SELECT files FROM representation_versions WHERE cloud_id = ? AND schema_id = ? AND version_id = ?;");
        getFilesStatement.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());

        getAllRepresentationsForRecordStatement = s
                .prepare("SELECT cloud_id, schema_id, version_id, provider_id, persistent, creation_date, files FROM representation_versions WHERE cloud_id = ? ORDER BY schema_id DESC, version_id DESC;");
        getAllRepresentationsForRecordStatement
                .setConsistencyLevel(connectionProvider.getConsistencyLevel());

        deleteRecordStatement = s.prepare("BEGIN BATCH "
                + "DELETE FROM representation_versions WHERE cloud_id = ? "
                + "DELETE FROM data_set_assignments WHERE cloud_id = ? "
                + "APPLY BATCH;");
        deleteRecordStatement.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());

        deleteRepresentationStatement = s
                .prepare("DELETE FROM representation_versions WHERE cloud_id = ? AND schema_id = ? ;");
        deleteRepresentationStatement.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());

        deleteRepresentationVersionStatement = s
                .prepare("DELETE FROM representation_versions WHERE cloud_id = ? AND schema_id = ? AND version_id = ? ;");
        deleteRepresentationVersionStatement
                .setConsistencyLevel(connectionProvider.getConsistencyLevel());

        singleRecordIdForProviderStatement = s
                .prepare("SELECT cloud_id FROM representation_versions WHERE provider_id = ? LIMIT 1;");
        singleRecordIdForProviderStatement
                .setConsistencyLevel(connectionProvider.getConsistencyLevel());

        getRepresentationRevisionStatement = s
                .prepare("SELECT version_id, files, revision_timestamp FROM representation_revisions WHERE cloud_id = ? AND representation_id = ? AND revision_provider_id = ? AND revision_name = ? AND revision_timestamp = ?;");
        getRepresentationRevisionStatement
                .setConsistencyLevel(connectionProvider.getConsistencyLevel());

        getLatestRepresentationRevisionStatement = s
                .prepare("SELECT version_id, files, revision_timestamp FROM representation_revisions WHERE cloud_id = ? AND representation_id = ? AND revision_provider_id = ? AND revision_name = ? LIMIT 1;");
        getLatestRepresentationRevisionStatement
                .setConsistencyLevel(connectionProvider.getConsistencyLevel());

        insertRepresentationRevisionStatement = s
                .prepare("INSERT INTO representation_revisions (cloud_id, representation_id, version_id, revision_provider_id, revision_name, revision_timestamp) VALUES (?,?,?,?,?,?);");
        insertRepresentationRevisionStatement.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());

        insertRepresentationRevisionFileStatement = s
                .prepare("UPDATE representation_revisions SET files[?] = ? WHERE cloud_id = ? AND representation_id = ? AND revision_provider_id = ? AND revision_name = ? AND revision_timestamp = ? AND version_id = ?;");
        insertRepresentationRevisionFileStatement.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());

        deleteRepresentationRevisionStatement = s
                .prepare("DELETE FROM representation_revisions WHERE cloud_id = ? AND representation_id = ? AND revision_provider_id = ? AND revision_name = ? AND revision_timestamp = ? AND version_id = ?");
        deleteRepresentationRevisionStatement.setConsistencyLevel(connectionProvider
                .getConsistencyLevel());
    }

    /**
     * Returns a record containing latest persistent representation in each
     * schema. If there is no persistent representation in some schema, it would
     * not be returned.
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
            rep.setFiles(deserializeFiles(row.getMap("files", String.class, String.class)));
            if (rep.isPersistent() && !rep.getRepresentationName().equals(prevSchema)) {
                representations.add(rep);
                prevSchema = rep.getRepresentationName();
            }
        }
        return new Record(cloudId, representations);
    }

    /**
     * Deletes record with all representations and all versions. If such record
     * doesn't exist - nothing happens.
     *
     * @param cloudId indentifier of record to be deleted.
     */
    public void deleteRecord(String cloudId) throws NoHostAvailableException,
            QueryExecutionException {
        BoundStatement boundStatement = deleteRecordStatement.bind(cloudId,
                cloudId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    /**
     * Deletes representation with all versions. If such represenation doesn't
     * exist - nothing happens.
     *
     * @param cloudId identifier of record
     * @param schema  schema of representation to be deleted.
     */
    public void deleteRepresentation(String cloudId, String schema)
            throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = deleteRepresentationStatement.bind(
                cloudId, schema);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    /**
     * Deletes record's representation in specified version. If such version
     * doesn't exist - nothing happens. This method doesn't check any
     * constraints e.g. can delete persistent version.
     *
     * @param cloudId identifier of record
     * @param schema  schema of representation
     * @param version version of representation
     */
    public void deleteRepresentation(String cloudId, String schema,
                                     String version) throws NoHostAvailableException,
            QueryExecutionException {
        BoundStatement boundStatement = deleteRepresentationVersionStatement
                .bind(cloudId, schema, UUID.fromString(version));
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    /**
     * Returns latest persistent version of record's representation. If there is
     * no such representation or no version of this representation is persistent
     * - will return null.
     *
     * @param cloudId identifier of record
     * @param schema  schema of representation
     * @return latest persistent version of a representation or null if such
     * doesn't exist.
     */
    public Representation getLatestPersistentRepresentation(String cloudId,
                                                            String schema) {
        List<Representation> allRepresentations;
        try {
            allRepresentations = this.listRepresentationVersions(cloudId,
                    schema);
            for (Representation r : allRepresentations) {
                if (r.isPersistent()) {
                    r.setFiles(getFilesForRepresentation(cloudId, schema,
                            r.getVersion()));
                    return r;
                }
            }
        } catch (RepresentationNotExistsException ex) { // don't rethrow, just
            // return null
            return null;
        }
        return null;
    }

    /**
     * Creates new temporary version for a specific record's representation.
     *
     * @param cloudId      identifier of record
     * @param schema       schema of representation
     * @param providerId   representation version provider
     * @param creationTime creation date
     * @return
     */
    public Representation createRepresentation(String cloudId, String schema,
                                               String providerId, Date creationTime)
            throws NoHostAvailableException, QueryExecutionException {
        if (cloudId == null || schema == null || providerId == null) {
            throw new IllegalArgumentException("Parameters cannot be null");
        }
        UUID version = getTimeUUID();

        // insert representation into representation table.
        BoundStatement boundStatement = insertRepresentationStatement.bind(
                cloudId, schema, version, providerId, false, creationTime);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        return new Representation(cloudId, schema, version.toString(), null,
                null, providerId, new ArrayList<File>(0), new ArrayList<Revision>(0), false, creationTime);
    }

    /**
     * Returns a representation of a record in specified schema and version.
     *
     * @param cloudId id of the record
     * @param schema  schema of the representation
     * @param version version of the representation.
     * @return representation.
     * @throws QueryExecutionException  if error occured while executing a query.
     * @throws NoHostAvailableException if no Cassandra host are available.
     */
    public Representation getRepresentation(String cloudId, String schema,
                                            String version) throws NoHostAvailableException,
            QueryExecutionException {
        if (cloudId == null || schema == null || version == null) {
            throw new IllegalArgumentException("Parameters cannot be null");
        }
        BoundStatement boundStatement = getRepresentationVersionStatement.bind(
                cloudId, schema, UUID.fromString(version));
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);

        QueryTracer.logConsistencyLevel(boundStatement, rs);

        Row row = rs.one();
        if (row == null) {
            return null;
        } else {
            Representation rep = mapToRepresentation(row);
            rep.setFiles(deserializeFiles(row.getMap("files", String.class,
                    String.class)));
            rep.setRevisions(deserializeRevisions(row.getMap("revisions", String.class,
                    String.class)));
            return rep;
        }
    }

    /**
     * Returns files for representation. If threre is no such representation -
     * will return null. If representation does not contain any files - will
     * return empty list.
     *
     * @param cloudId
     * @param schema
     * @param version
     * @return
     */
    public List<File> getFilesForRepresentation(String cloudId, String schema,
                                                String version) throws NoHostAvailableException,
            QueryExecutionException {
        BoundStatement boundStatement = getFilesStatement.bind(cloudId, schema,
                UUID.fromString(version));
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        Row row = rs.one();
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        if (row == null) {
            return null;
        } else {
            Map<String, String> fileNameToFile = row.getMap("files",
                    String.class, String.class);
            return deserializeFiles(fileNameToFile);
        }
    }

    /**
     * Makes a certain temporary representation version a persistent one. Sets
     * creation date.
     *
     * @param cloudId      id of the record
     * @param schema       schema of the representation
     * @param version      version of the representation
     * @param creationTime date of creation
     * @throws QueryExecutionException  if error occured while executing a query.
     * @throws NoHostAvailableException if no Cassandra host are available.
     */
    public void persistRepresentation(String cloudId, String schema,
                                      String version, Date creationTime) throws NoHostAvailableException,
            QueryExecutionException {
        BoundStatement boundStatement = persistRepresentationStatement.bind(
                creationTime, cloudId, schema, UUID.fromString(version));
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    /**
     * Returns all versions of representation (persistent or not), sorted from
     * the most recent to the oldes. If no representation exist - will return
     * empty list.
     *
     * @param cloudId record id
     * @param schema  schema id
     * @return
     * @throws RepresentationNotExistsException when there is no representation matching requested parameters
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
            Representation representation = mapToRepresentation(row);
            representation.setFiles(deserializeFiles(row.getMap("files", String.class, String.class)));
            representation.setRevisions(deserializeRevisions(row.getMap("revisions", String.class, String.class)));
            result.add(representation);
        }
        return result;
    }

    /**
     * Returns all versions of all representations (persistent or not) for a
     * cloud id.
     *
     * @param cloudId record id
     * @return
     */
    public List<Representation> listRepresentationVersions(String cloudId)
            throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = listRepresentationVersionsAllSchemasStatement.bind(cloudId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        List<Representation> result = new ArrayList<>(rs.getAvailableWithoutFetching());
        for (Row row : rs) {
            Representation representation = mapToRepresentation(row);
            representation.setFiles(deserializeFiles(row.getMap("files", String.class, String.class)));
            result.add(representation);
        }
        return result;
    }

    /**
     * Adds or modifies given file to list of files of representation.
     *
     * @param cloudId record if
     * @param schema  schema id
     * @param version version id
     * @param file    file
     * @throws QueryExecutionException  if error occured while executing a query.
     * @throws NoHostAvailableException if no Cassandra host are available.
     */
    public void addOrReplaceFileInRepresentation(String cloudId, String schema,
                                                 String version, File file) throws NoHostAvailableException,
            QueryExecutionException {
        BoundStatement boundStatement = insertFileStatement.bind(
                file.getFileName(), serializeFile(file), cloudId, schema,
                UUID.fromString(version));
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    /**
     * Removes file entry from list of files belonging to record representation.
     *
     * @param cloudId  record if
     * @param schema   schema id
     * @param version  version id
     * @param fileName name of file to be removed from representation
     * @throws QueryExecutionException  if error occured while executing a query.
     * @throws NoHostAvailableException if no Cassandra host are available.
     */
    public void removeFileFromRepresentation(String cloudId, String schema,
                                             String version, String fileName) throws NoHostAvailableException,
            QueryExecutionException {
        BoundStatement boundStatement = removeFileStatement.bind(fileName,
                cloudId, schema, UUID.fromString(version));
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    /**
     * Checks if given provider has any representations.
     *
     * @param providerId identifier of the provider
     * @return true if provider has representations, false otherwise
     * @throws QueryExecutionException  if error occured while executing a query.
     * @throws NoHostAvailableException if no Cassandra host are available.
     */
    public boolean providerHasRepresentations(String providerId)
            throws NoHostAvailableException, QueryExecutionException {
        BoundStatement boundStatement = singleRecordIdForProviderStatement
                .bind(providerId);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        Row row = rs.one();
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        return row != null;
    }

    private Representation mapToRepresentation(Row row) {
        Representation representation = new Representation();
        representation.setDataProvider(row.getString("provider_id"));
        representation.setCloudId(row.getString("cloud_id"));
        representation.setRepresentationName(row.getString("schema_id"));
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

    private List<Revision> deserializeRevisions(Map<String, String> revisionNametoRevision) {
        if (revisionNametoRevision == null) {
            return new ArrayList<>(0);
        }
        List<Revision> revisions = new ArrayList<>(revisionNametoRevision.size());
        for (String revisionJSON : revisionNametoRevision.values()) {
            revisions.add(revisionGson.fromJson(revisionJSON, Revision.class));
        }
        return revisions;
    }

    private String serializeFile(File f) {
        f.setContentUri(null);
        return gson.toJson(f);
    }

    private String serializeRevision(Revision revision) {
        return revisionGson.toJson(revision);
    }

    private static UUID getTimeUUID() {
        return UUID.fromString(new com.eaio.uuid.UUID().toString());
    }

    /**
     * Adds or modifies given revision to list of revisions of representation version.
     *
     * @param cloudId  record if
     * @param schema   schema id
     * @param version  version id
     * @param revision Revision
     * @throws QueryExecutionException  if error occured while executing a query.
     * @throws NoHostAvailableException if no Cassandra host are available.
     */
    public void addOrReplaceRevisionInRepresentation(String cloudId, String schema,
                                                     String version, Revision revision) throws NoHostAvailableException,
            QueryExecutionException, RevisionIsNotValidException {
        validateRevision(revision);
        BoundStatement boundStatement = insertRevisionStatement.bind(RevisionUtils.getRevisionKey(revision)
                , serializeRevision(revision), cloudId, schema,
                UUID.fromString(version));
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    void validateRevision(Revision revision) throws RevisionIsNotValidException {
        if (revision == null)
            throw new RevisionIsNotValidException("Revision can't be null");
        else {
            if (revision.getRevisionProviderId() == null)
                throw new RevisionIsNotValidException("Revision should include revisionProviderId");
            else if (revision.getRevisionName() == null)
                throw new RevisionIsNotValidException("Revision should include revisionName");
            else if (revision.getCreationTimeStamp() == null)
                throw new RevisionIsNotValidException("Revision should include creationTimestamp");
            else if (revision.getUpdateTimeStamp() == null)
                throw new RevisionIsNotValidException("Revision should include updateTimestamp");
        }
    }

    public RepresentationRevisionResponse getRepresentationRevision(String cloudId, String schema, String revisionProviderId, String revisionName, Date revisionTimestamp) {
        // check parameters, none can be null
        if (cloudId == null || schema == null || revisionProviderId == null || revisionName == null) {
            throw new IllegalArgumentException("Parameters cannot be null");
        }
        BoundStatement boundStatement;

        // bind parameters to statement
        if (revisionTimestamp != null)
            boundStatement = getRepresentationRevisionStatement.bind(
                    cloudId, schema, revisionProviderId, revisionName, revisionTimestamp);
        else
            boundStatement = getLatestRepresentationRevisionStatement.bind(cloudId, schema, revisionProviderId, revisionName);

        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);

        // retrieve one row, there should be only one
        Row row = rs.one();
        if (row != null) {
            // prepare representation revision object from the fields in a row
            RepresentationRevisionResponse representationRevision = new RepresentationRevisionResponse(cloudId, schema,
                    row.getUUID("version_id").toString(), revisionProviderId, revisionName, row.getDate("revision_timestamp"));
            // retrieve files information from the map
            representationRevision.setFiles(deserializeFiles(row.getMap("files", String.class,
                    String.class)));
            return representationRevision;
        }
        return null;
    }


    /**
     * Adds or modifies given file to list of files of representation revision.
     *
     * @param cloudId record if
     * @param schema  schema id
     * @param version version id
     * @param revisionProviderId   revision provider identifier
     * @param revisionName revision name
     * @param revisionTimestamp revision timestamp
     * @param file    file
     * @throws QueryExecutionException  if error occured while executing a query.
     * @throws NoHostAvailableException if no Cassandra host are available.
     */
    public void addOrReplaceFileInRepresentationRevision(String cloudId, String schema,
                                                 String version, String revisionProviderId, String revisionName, Date revisionTimestamp, File file) throws NoHostAvailableException,
            QueryExecutionException {
        BoundStatement boundStatement = insertRepresentationRevisionFileStatement.bind(
                file.getFileName(), serializeFile(file), cloudId, schema, revisionProviderId, revisionName, revisionTimestamp,
                UUID.fromString(version));
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }


    /**
     * Adds new tuple in table storing associations between representations and revisions
     *
     * @param cloudId      identifier of record
     * @param schema       schema of representation
     * @param version      version identifier
     * @param revisionProviderId   revision provider identifier
     * @param revisionName revision name
     * @param revisionTimestamp revision timestamp
     * @return
     */
    public RepresentationRevisionResponse addRepresentationRevision(String cloudId, String schema,
                                                                    String version, String revisionProviderId, String revisionName, Date revisionTimestamp)
            throws NoHostAvailableException, QueryExecutionException {

        // none of the parameters can be null
        validateParameters(cloudId, schema, version, revisionProviderId, revisionName, revisionTimestamp);

        // insert representation revision into representation revision table.
        BoundStatement boundStatement = insertRepresentationRevisionStatement.bind(
                cloudId, schema, UUID.fromString(version), revisionProviderId, revisionName, revisionTimestamp);
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
        return new RepresentationRevisionResponse(cloudId, schema, version, new ArrayList<File>(0), revisionProviderId, revisionName, revisionTimestamp);
    }


    /**
     * Deletes tuple from the table holding associations between representations and revisions
     *
     * @param cloudId      identifier of record
     * @param schema       schema of representation
     * @param version      version identifier
     * @param revisionProviderId   revision provider identifier
     * @param revisionName revision name
     * @param revisionTimestamp revision timestamp
     * @return
     */
    public void deleteRepresentationRevision(String cloudId, String schema,
                                                                    String version, String revisionProviderId, String revisionName, Date revisionTimestamp)
            throws NoHostAvailableException, QueryExecutionException {

        validateParameters(cloudId, schema, version, revisionProviderId, revisionName, revisionTimestamp);

        // delete representation revision into representation revision table.
        BoundStatement boundStatement = deleteRepresentationRevisionStatement.bind(
                cloudId, schema, revisionProviderId, revisionName, revisionTimestamp, UUID.fromString(version));
        ResultSet rs = connectionProvider.getSession().execute(boundStatement);
        QueryTracer.logConsistencyLevel(boundStatement, rs);
    }

    private void validateParameters(String cloudId, String schema, String version, String revisionProviderId, String revisionName, Date revisionTimestamp) {
        // none of the parameters can be null
        if (cloudId == null || schema == null || version == null || revisionProviderId == null || revisionName == null || revisionTimestamp == null) {
            throw new IllegalArgumentException("Parameters cannot be null");
        }
    }
}
