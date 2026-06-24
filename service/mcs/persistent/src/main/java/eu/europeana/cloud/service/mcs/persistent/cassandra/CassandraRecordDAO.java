package eu.europeana.cloud.service.mcs.persistent.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.google.gson.Gson;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.common.model.*;
import eu.europeana.cloud.common.model.Record;
import eu.europeana.cloud.service.mcs.exception.RepresentationNotExistsException;
import eu.europeana.cloud.service.mcs.persistent.util.QueryTracer;
import jakarta.annotation.PostConstruct;

import java.util.*;

import static eu.europeana.cloud.service.mcs.key.RepresentationVersionsDbColumnNames.*;


/**
 * Repository for records, their representations and versions. Uses Cassandra as storage.
 */
@Retryable
public class CassandraRecordDAO {
  private static final String MSG_PARAMETERS_CANNOT_BE_NULL = "Parameters cannot be null";

  // json serializer/deserializer
  private final Gson gson = new Gson();
  private final CassandraConnectionProvider connectionProvider;
  private PreparedStatement insertRepresentationStatement;
  private PreparedStatement deleteRepresentationVersionStatement;
  private PreparedStatement deleteRepresentationStatement;
  private PreparedStatement getRepresentationVersionStatement;
  private PreparedStatement getRepresentationDatasetIdAndProviderIdStatement;
  private PreparedStatement listRepresentationVersionsStatement;
  private PreparedStatement listRepresentationVersionsAllSchemasStatement;
  private PreparedStatement persistRepresentationStatement;
  private PreparedStatement insertFileStatement;
  private PreparedStatement removeFileStatement;
  private PreparedStatement getFilesStatement;
  private PreparedStatement getAllRepresentationsForRecordStatement;

  public CassandraRecordDAO(CassandraConnectionProvider connectionProvider) {
    this.connectionProvider = connectionProvider;
  }

  /**
   * Returns a record containing latest persistent representation in each schema. If there is no persistent representation in some
   * schema, it would not be returned.
   *
   * @param cloudId
   * @return
   */
  public Record getRecord(String cloudId) throws NoHostAvailableException, QueryExecutionException {

    final BoundStatement boundStatement = getAllRepresentationsForRecordStatement.bind(cloudId);
    ResultSet rs = connectionProvider.getSession().execute(boundStatement);
    QueryTracer.logConsistencyLevel(boundStatement, rs);
    List<Representation> representations = new ArrayList<>();
    String prevSchema = null;
    for (Row row : rs) {
      Representation rep = mapToRepresentation(row);
      rep.setFiles(deserializeFiles(row.getMap(FILES, String.class, String.class)));
      if (rep.isPersistent() && !rep.getRepresentationName().equals(prevSchema)) {
        representations.add(rep);
        prevSchema = rep.getRepresentationName();
      }
    }
    return new Record(cloudId, representations);
  }

  /**
   * Deletes representation with all versions. If such represenation doesn't exist - nothing happens.
   *
   * @param cloudId identifier of record
   * @param schema schema of representation to be deleted.
   */
  public void deleteRepresentation(String cloudId, String schema)
      throws NoHostAvailableException, QueryExecutionException {

    BoundStatement boundStatement = deleteRepresentationStatement.bind(cloudId, schema);
    ResultSet rs = connectionProvider.getSession().execute(boundStatement);
    QueryTracer.logConsistencyLevel(boundStatement, rs);
  }

  /**
   * Deletes record's representation in specified version. If such version doesn't exist - nothing happens. This method doesn't
   * check any constraints e.g. can delete persistent version.
   *
   * @param cloudId identifier of record
   * @param schema schema of representation
   * @param version version of representation
   */
  public void deleteRepresentation(String cloudId, String schema, String version)
      throws NoHostAvailableException, QueryExecutionException {

    BoundStatement boundStatement = deleteRepresentationVersionStatement.bind(cloudId, schema, UUID.fromString(version));
    ResultSet rs = connectionProvider.getSession().execute(boundStatement);
    QueryTracer.logConsistencyLevel(boundStatement, rs);
  }

  /**
   * Returns latest persistent version of record's representation. If there is no such representation or no version of this
   * representation is persistent - will return null.
   *
   * @param cloudId identifier of record
   * @param schema schema of representation
   * @return latest persistent version of a representation or null if such doesn't exist.
   */
  public Representation getLatestPersistentRepresentation(String cloudId, String schema) {

    List<Representation> allRepresentations = this.listRepresentationVersions(cloudId, schema);

    if (allRepresentations != null) {
      for (Representation r : allRepresentations) {
        if (r.isPersistent()) {
          r.setFiles(getFilesForRepresentation(cloudId, schema, r.getVersion()));
          return r;
        }
      }
    }

    return null;
  }

  /**
   * Creates new temporary version for a specific record's representation.
   *
   * @param cloudId identifier of record
   * @param schema schema of representation
   * @param providerId representation version provider
   * @param creationTime creation date
   * @param version representation version
   * @param datasetId dataset id
   * @return newly created representation
   * @throws QueryExecutionException if error occured while executing a query.
   * @throws NoHostAvailableException if no Cassandra host are available.
   */
  public Representation createRepresentation(String cloudId, String schema, String providerId, Date creationTime, UUID version, String datasetId, boolean markDepublished)
      throws NoHostAvailableException, QueryExecutionException {
    if (cloudId == null || schema == null || providerId == null) {
      throw new IllegalArgumentException(MSG_PARAMETERS_CANNOT_BE_NULL);
    }

    // insert representation into representation table.
    BoundStatement boundStatement = insertRepresentationStatement.bind(
            cloudId, schema, version, providerId, false, creationTime, datasetId, markDepublished);
    ResultSet rs = connectionProvider.getSession().execute(boundStatement);
    QueryTracer.logConsistencyLevel(boundStatement, rs);
    return new Representation(cloudId, schema, version.toString(), null, null, providerId,
        new ArrayList<>(0), false, creationTime, datasetId, markDepublished);
  }

  /**
   * Returns a representation of a record in specified schema and version.
   *
   * @param cloudId id of the record
   * @param schema schema of the representation
   * @param version version of the representation.
   * @return representation.
   * @throws QueryExecutionException if error occured while executing a query.
   * @throws NoHostAvailableException if no Cassandra host are available.
   */
  public Representation getRepresentation(String cloudId, String schema, String version)
      throws NoHostAvailableException, QueryExecutionException {

    if (cloudId == null || schema == null || version == null) {
      throw new IllegalArgumentException(MSG_PARAMETERS_CANNOT_BE_NULL);
    }
    BoundStatement boundStatement = getRepresentationVersionStatement.bind(cloudId, schema, UUID.fromString(version));
    ResultSet rs = connectionProvider.getSession().execute(boundStatement);

    QueryTracer.logConsistencyLevel(boundStatement, rs);

    Row row = rs.one();
    if (row == null) {
      return null;
    } else {
      Representation rep = mapToRepresentation(row);
      rep.setFiles(deserializeFiles(row.getMap(FILES, String.class,
          String.class)));
      return rep;
    }
  }

  /**
   * Returns a dataset id associated to representation.
   *
   * @param cloudId id of the record
   * @param schema schema of the representation
   * @return Optional of dataset id.
   * @throws QueryExecutionException if error occurred while executing a query.
   * @throws NoHostAvailableException if no Cassandra host are available.
   */
  public Optional<CompoundDataSetId> getRepresentationDatasetId(String cloudId, String schema, UUID version)
          throws NoHostAvailableException, QueryExecutionException {

    if (cloudId == null || schema == null) {
      throw new IllegalArgumentException(MSG_PARAMETERS_CANNOT_BE_NULL);
    }
    BoundStatement boundStatement = getRepresentationDatasetIdAndProviderIdStatement.bind(cloudId, schema, version);
    ResultSet rs = connectionProvider.getSession().execute(boundStatement);

    QueryTracer.logConsistencyLevel(boundStatement, rs);

    if (!rs.isExhausted()) {
      Row row = rs.one();
      String datasetId = row.getString(DATASET_ID);
      String providerId = row.getString(PROVIDER_ID);
      return Optional.of(new CompoundDataSetId(providerId, datasetId));
    } else {
      return Optional.empty();
    }
  }

  /**
   * Returns files for representation. If threre is no such representation - will return null. If representation does not contain
   * any files - will return empty list.
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
      Map<String, String> fileNameToFile = row.getMap(FILES, String.class, String.class);
      return deserializeFiles(fileNameToFile);
    }
  }

  /**
   * Makes a certain temporary representation version a persistent one. Sets creation date.
   *
   * @param cloudId id of the record
   * @param schema schema of the representation
   * @param version version of the representation
   * @param creationTime date of creation
   * @throws QueryExecutionException if error occured while executing a query.
   * @throws NoHostAvailableException if no Cassandra host are available.
   */
  public void persistRepresentation(String cloudId, String schema, String version, Date creationTime)
      throws NoHostAvailableException, QueryExecutionException {

    BoundStatement boundStatement = persistRepresentationStatement.bind(
        creationTime, cloudId, schema, UUID.fromString(version));

    ResultSet rs = connectionProvider.getSession().execute(boundStatement);
    QueryTracer.logConsistencyLevel(boundStatement, rs);
  }

  /**
   * Returns all versions of representation (persistent or not), sorted from the most recent to the oldes. If no representation
   * exist - will return empty list.
   *
   * @param cloudId record id
   * @param schema schema id
   * @return
   * @throws RepresentationNotExistsException when there is no representation matching requested parameters
   */
  public List<Representation> listRepresentationVersions(String cloudId, String schema)
      throws NoHostAvailableException, QueryExecutionException {

    BoundStatement boundStatement = listRepresentationVersionsStatement.bind(cloudId, schema);
    ResultSet rs = connectionProvider.getSession().execute(boundStatement);
    QueryTracer.logConsistencyLevel(boundStatement, rs);

    if (rs.isExhausted()) {
      return null;
    }
    List<Representation> result = new ArrayList<>(rs.getAvailableWithoutFetching());
    mapResultSetToRepresentationList(rs, result);
    return result;
  }

  /**
   * Returns all versions of all representations (persistent or not) for a cloud id.
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
    mapResultSetToRepresentationList(rs, result);
    return result;
  }

  /**
   * Adds or modifies given file to list of files of representation.
   *
   * @param cloudId record if
   * @param schema schema id
   * @param version version id
   * @param file file
   * @throws QueryExecutionException if error occured while executing a query.
   * @throws NoHostAvailableException if no Cassandra host are available.
   */
  public void addOrReplaceFileInRepresentation(String cloudId, String schema, String version, File file)
      throws NoHostAvailableException, QueryExecutionException {

    BoundStatement boundStatement = insertFileStatement.bind(
        file.getFileName(), serializeFile(file), cloudId, schema, UUID.fromString(version));

    ResultSet rs = connectionProvider.getSession().execute(boundStatement);
    QueryTracer.logConsistencyLevel(boundStatement, rs);
  }

  /**
   * Removes file entry from list of files belonging to record representation.
   *
   * @param cloudId record if
   * @param schema schema id
   * @param version version id
   * @param fileName name of file to be removed from representation
   * @throws QueryExecutionException if error occured while executing a query.
   * @throws NoHostAvailableException if no Cassandra host are available.
   */
  public void removeFileFromRepresentation(String cloudId, String schema, String version, String fileName)
      throws NoHostAvailableException, QueryExecutionException {

    BoundStatement boundStatement = removeFileStatement.bind(fileName,
        cloudId, schema, UUID.fromString(version));
    ResultSet rs = connectionProvider.getSession().execute(boundStatement);
    QueryTracer.logConsistencyLevel(boundStatement, rs);
  }

    //  Need separate function so mock in test can modify it
    @PostConstruct
    private void postConstruct() {
        prepareStatements();
    }

    void prepareStatements() {
        Session session = connectionProvider.getSession();

        insertRepresentationStatement = session.prepare(
                "INSERT INTO " +
                        "representation_versions (cloud_id, schema_id, version_id, provider_id, persistent, creation_date, dataset_id, mark_deleted) " +
                        "VALUES (?,?,?,?,?,?,?,?);"
        );

        getRepresentationVersionStatement = session.prepare(
                "SELECT cloud_id, schema_id, version_id, provider_id, persistent, creation_date, files, dataset_id, mark_deleted " +
            "FROM representation_versions " +
            "WHERE cloud_id = ? AND schema_id = ? AND version_id = ?;"
    );

    getRepresentationDatasetIdAndProviderIdStatement = session.prepare(
            "SELECT dataset_id, provider_id" +
                    " FROM representation_versions " +
                    "WHERE cloud_id = ? AND schema_id = ? AND version_id=?" +
                    "LIMIT 1;"
    );

    listRepresentationVersionsStatement = session.prepare(
        "SELECT cloud_id, schema_id, version_id, provider_id, persistent, creation_date, files, dataset_id, mark_deleted " +
            "FROM representation_versions " +
            "WHERE cloud_id = ? AND schema_id = ? " +
            "ORDER BY schema_id DESC, version_id DESC;"
    );

    listRepresentationVersionsAllSchemasStatement = session.prepare(
        "SELECT cloud_id, schema_id, version_id, provider_id, persistent, creation_date, files, dataset_id, mark_deleted " +
            "FROM representation_versions " +
            "WHERE cloud_id = ?;"
    );

    persistRepresentationStatement = session.prepare(
        "UPDATE representation_versions " +
            "SET persistent = TRUE, creation_date = ? " +
            "WHERE cloud_id = ? AND schema_id=? AND version_id = ?;"
    );

    insertFileStatement = session.prepare(
        "UPDATE representation_versions " +
            "SET files[?] = ? " +
            "WHERE cloud_id = ? AND schema_id = ? AND version_id = ?;"
    );

    removeFileStatement = session.prepare(
        "DELETE files[?] " +
            "FROM representation_versions " +
            "WHERE cloud_id = ? AND schema_id = ? AND version_id = ?;"
    );

    getFilesStatement = session.prepare(
        "SELECT files " +
            "FROM representation_versions " +
            "WHERE cloud_id = ? AND schema_id = ? AND version_id = ?;"
    );

    getAllRepresentationsForRecordStatement = session.prepare(
        "SELECT cloud_id, schema_id, version_id, provider_id, persistent, creation_date, files, dataset_id, mark_deleted " +
            "FROM representation_versions " +
            "WHERE cloud_id = ? " +
            "ORDER BY schema_id DESC, version_id DESC;"
    );

    deleteRepresentationStatement = session.prepare(
        "DELETE " +
            "FROM representation_versions " +
            "WHERE cloud_id = ? AND schema_id = ?;"
    );

    deleteRepresentationVersionStatement = session.prepare(
        "DELETE " +
            "FROM representation_versions " +
            "WHERE cloud_id = ? AND schema_id = ? AND version_id = ?;"
    );

  }

  private void mapResultSetToRepresentationList(ResultSet rs, List<Representation> result) {
    for (Row row : rs) {
      Representation representation = mapToRepresentation(row);
      representation.setFiles(deserializeFiles(row.getMap(FILES, String.class, String.class)));
      result.add(representation);
    }
  }

  private Representation mapToRepresentation(Row row) {
    Representation representation = new Representation();
    representation.setDataProvider(row.getString(PROVIDER_ID));
    representation.setCloudId(row.getString(CLOUD_ID));
    representation.setRepresentationName(row.getString(SCHEMA_ID));
    representation.setVersion(row.getUUID(VERSION_ID).toString());
    representation.setPersistent(row.getBool(PERSISTENT));
    representation.setCreationDate(row.getTimestamp(CREATION_DATE));
    representation.setDatasetId(row.getString(DATASET_ID));
      representation.setMarkDepublished(row.getBool(MARK_DELETED));
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

}
