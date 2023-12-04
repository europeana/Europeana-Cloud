/* Copyright 2013 Rigas Grigoropoulos
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package eu.europeana.aas.authorization.repository;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import eu.europeana.aas.authorization.model.AclEntry;
import eu.europeana.aas.authorization.model.AclObjectIdentity;
import eu.europeana.aas.authorization.repository.exceptions.AclNotFoundException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import eu.europeana.cloud.service.commons.utils.RetryableMethodExecutor;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;

/**
 * Implementation of <code>AclRepository</code> using the DataStax Java Driver.
 *
 * @author Rigas Grigoropoulos
 * @author Markus.Muhr@theeuropeanlibrary.org
 */

public class CassandraAclRepository implements AclRepository {

  private static final Log LOG = LogFactory
      .getLog(CassandraAclRepository.class);

  private static final String AOI_TABLE = "aois";
  private static final String ACL_TABLE = "acls";
  private static final String ACL_TABLE_SID_FIELD = "sid";
  private static final String ACL_TABLE_ACL_ORDER_FIELD = "aclorder";

  private static final String CHILDREN_TABLE = "children";
  private static final String CHILDREN_TABLE_CHILD_ID_FIELD = "childId";

  private static final String COMMON_OBJ_ID_FIELD = "objId";
  private static final String COMMON_OBJ_CLASS_FIELD = "objClass";
  private static final String COMMON_ID_FIELD = "id";
  
  private static final String[] AOI_KEYS = new String[]{"id", COMMON_OBJ_ID_FIELD,
          COMMON_OBJ_CLASS_FIELD, "isInheriting", "owner", "isOwnerPrincipal",
      "parentObjId", "parentObjClass"};
  private static final String[] CHILD_KEYS = new String[]{"id", CHILDREN_TABLE_CHILD_ID_FIELD,
          COMMON_OBJ_ID_FIELD, COMMON_OBJ_CLASS_FIELD};
  private static final String[] ACL_KEYS = new String[]{"id", "aclOrder",
      "sid", "mask", "isSidPrincipal", "isGranting", "isAuditSuccess",
      "isAuditFailure"};

  private static final String ERROR_MASSAGE_IN_CASE_ALL_RETRY_FAILED
      = "Repository could now establish connection to cassandra database";
  static final int ACL_REPO_DEFAULT_MAX_ATTEMPTS = 3;

  private RegularStatement createChildrenTable;
  private RegularStatement createAoisTable;
  private RegularStatement createAclsTable;

  private final Session session;
  private final String keyspace;


  /**
   * Constructs a new <code>CassandraAclRepositoryImpl</code>.
   *
   * @param provider providing the <code>Session</code> to use for connectivity with Cassandra.
   * @param initSchema whether the keyspace and schema for storing ACLs should // * be created.
   */
  public CassandraAclRepository(CassandraConnectionProvider provider, boolean initSchema) {
    this(provider.getSession(), provider.getKeyspaceName(), initSchema);
  }

  /**
   * Constructs a new <code>CassandraAclRepositoryImpl</code>.
   *
   * @param session the <code>Session</code> to use for connectivity with Cassandra.
   * @param keyspace whether the keyspace and schema for storing ACLs should be created.
   */
  public CassandraAclRepository(Session session, String keyspace) {
    this.session = session;
    this.keyspace = keyspace;
    initStatements();
  }

  /**
   * Constructs a new <code>CassandraAclRepositoryImpl</code> and optionally creates the Cassandra keyspace and schema for storing
   * ACLs.
   *
   * @param session the <code>Session</code> to use for connectivity with Cassandra.
   * @param keyspace whether the keyspace and schema for storing ACLs should be created.
   * @param initSchema whether the keyspace and schema for storing ACLs should be created.
   */
  public CassandraAclRepository(Session session, String keyspace, boolean initSchema) {
    this(session, keyspace);
    if (initSchema) {
      createAoisTable();
      createChildrenTable();
      createAclsTable();
    }
  }


  private void initStatements() {
    createChildrenTable = new SimpleStatement("CREATE TABLE children (" + "id varchar," + "childId varchar," + "objId varchar,"
        + "objClass varchar," + "PRIMARY KEY (id, childId)" + ");");
    createAoisTable = new SimpleStatement(
        "CREATE TABLE aois (" + "id varchar PRIMARY KEY," + "objId varchar," + "objClass varchar,"
            + "isInheriting boolean," + "owner varchar," + "isOwnerPrincipal boolean," + "parentObjId varchar,"
            + "parentObjClass varchar" + ");");
    createAclsTable = new SimpleStatement("CREATE TABLE acls (" + "id varchar," + "sid varchar," + "aclOrder int," + "mask int,"
        + "isSidPrincipal boolean," + "isGranting boolean," + "isAuditSuccess boolean,"
        + "isAuditFailure boolean," + "PRIMARY KEY (id, sid, aclOrder)" + ");");
  }


  private ResultSet executeStatement(Session session, RegularStatement statement) {
    return session.execute(statement);
  }

  @Override
  @Retryable(maxAttempts = ACL_REPO_DEFAULT_MAX_ATTEMPTS, errorMessage = ERROR_MASSAGE_IN_CASE_ALL_RETRY_FAILED)
  public Map<AclObjectIdentity, Set<AclEntry>> findAcls(List<AclObjectIdentity> objectIdsToLookup) {
    assertAclObjectIdentityList(objectIdsToLookup);

    if (LOG.isDebugEnabled()) {
      LOG.debug("BEGIN findAcls: objectIdentities: " + objectIdsToLookup);
    }

    List<String> ids = new ArrayList<>(objectIdsToLookup.size());
    for (AclObjectIdentity entry : objectIdsToLookup) {
      ids.add(entry.getRowId());
    }
    ResultSet resultSet = executeStatement(session, QueryBuilder.select().all().from(keyspace, AOI_TABLE)
                                                                .where(QueryBuilder.in("id", ids.toArray())));

    Map<AclObjectIdentity, Set<AclEntry>> resultMap = new HashMap<>();
    for (Row row : resultSet.all()) {
      resultMap.put(convertToFullAclObjectIdentity(row), new TreeSet<>(
          Comparator.comparingInt(AclEntry::getOrder)));
    }

    resultSet = executeStatement(session, QueryBuilder.select().all().from(keyspace, ACL_TABLE)
                                                      .where(QueryBuilder.in("id", ids.toArray())));
    for (Row row : resultSet.all()) {
      String aoiId = row.getString("id");

      AclEntry aclEntry = new AclEntry();
      aclEntry.setAuditFailure(row.getBool("isAuditFailure"));
      aclEntry.setAuditSuccess(row.getBool("isAuditSuccess"));
      aclEntry.setGranting(row.getBool("isGranting"));
      aclEntry.setMask(row.getInt("mask"));
      aclEntry.setOrder(row.getInt("aclOrder"));
      aclEntry.setSid(row.getString("sid"));
      aclEntry.setSidPrincipal(row.getBool("isSidPrincipal"));
      aclEntry.setId(aoiId + ":" + aclEntry.getSid() + ":" + aclEntry.getOrder());

      for (Entry<AclObjectIdentity, Set<AclEntry>> entry : resultMap.entrySet()) {
        if (entry.getKey().getRowId().equals(aoiId)) {
          entry.getValue().add(aclEntry);
          break;
        }
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("END findAcls: objectIdentities: " + resultMap.keySet() + ", aclEntries: " + resultMap.values());
    }
    return resultMap;
  }

  @Override
  @Retryable(maxAttempts = ACL_REPO_DEFAULT_MAX_ATTEMPTS, errorMessage = ERROR_MASSAGE_IN_CASE_ALL_RETRY_FAILED)
  public AclObjectIdentity findAclObjectIdentity(AclObjectIdentity objectId) {
    assertAclObjectIdentity(objectId);

    if (LOG.isDebugEnabled()) {
      LOG.debug("BEGIN findAclObjectIdentity: objectIdentity: " + objectId);
    }

    Row row = executeStatement(session,
        QueryBuilder.select().all().from(keyspace, AOI_TABLE)
                    .where(QueryBuilder.eq("id", objectId.getRowId())))
        .one();
    AclObjectIdentity objectIdentity = convertToFullAclObjectIdentity(row);

    if (LOG.isDebugEnabled()) {
      LOG.debug("END findAclObjectIdentity: objectIdentity: " + objectIdentity);
    }
    return objectIdentity;
  }

  @Override
  @Retryable(maxAttempts = ACL_REPO_DEFAULT_MAX_ATTEMPTS, errorMessage = ERROR_MASSAGE_IN_CASE_ALL_RETRY_FAILED)
  public List<AclObjectIdentity> findAclObjectIdentityChildren(AclObjectIdentity objectId) {
    assertAclObjectIdentity(objectId);

    if (LOG.isDebugEnabled()) {
      LOG.debug("BEGIN findAclObjectIdentityChildren: objectIdentity: " + objectId);
    }

    ResultSet resultSet = executeStatement(session, QueryBuilder.select().all().from(keyspace, CHILDREN_TABLE)
                                                                .where(QueryBuilder.eq("id", objectId.getRowId())));

    List<AclObjectIdentity> result = new ArrayList<>();
    for (Row row : resultSet.all()) {
      result.add(convertToSimpleAclObjectIdentity(row));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("END findAclObjectIdentityChildren: children: " + result);
    }
    return result;
  }

  @Override
  @Retryable(maxAttempts = ACL_REPO_DEFAULT_MAX_ATTEMPTS, errorMessage = ERROR_MASSAGE_IN_CASE_ALL_RETRY_FAILED)
  public void deleteAcls(List<AclObjectIdentity> objectIdsToDelete) {
    assertAclObjectIdentityList(objectIdsToDelete);

    if (LOG.isDebugEnabled()) {
      LOG.debug("BEGIN deleteAcls: objectIdsToDelete: " + objectIdsToDelete);
    }

    List<String> ids = new ArrayList<>(objectIdsToDelete.size());
    for (AclObjectIdentity entry : objectIdsToDelete) {
      ids.add(entry.getRowId());
    }
    Batch batch = QueryBuilder.batch();
    batch.add(QueryBuilder.delete().all().from(keyspace, AOI_TABLE)
                          .where(QueryBuilder.in("id", ids.toArray())));
    batch.add(QueryBuilder.delete().all().from(keyspace, CHILDREN_TABLE)
                          .where(QueryBuilder.in("id", ids.toArray())));
    executeStatement(session, batch);

    if (LOG.isDebugEnabled()) {
      LOG.debug("END deleteAcls");
    }
  }

  @Override
  @Retryable(maxAttempts = ACL_REPO_DEFAULT_MAX_ATTEMPTS, errorMessage = ERROR_MASSAGE_IN_CASE_ALL_RETRY_FAILED)
  public void saveAcl(AclObjectIdentity aoi) {
    assertAclObjectIdentity(aoi);
    if (LOG.isDebugEnabled()) {
      LOG.debug("BEGIN saveAcl: aclObjectIdentity: " + aoi);
    }

    Batch batch = QueryBuilder.batch();
    batch.add(QueryBuilder.insertInto(keyspace, AOI_TABLE)
                          .values(AOI_KEYS,
                              new Object[]{aoi.getRowId(), aoi.getId(), aoi.getObjectClass(), aoi.isEntriesInheriting(),
                                  aoi.getOwnerId(), aoi.isOwnerPrincipal(), aoi.getParentObjectId(),
                                  aoi.getParentObjectClass()}));

    if (aoi.getParentRowId() != null) {
      batch.add(QueryBuilder.insertInto(keyspace, CHILDREN_TABLE).values(CHILD_KEYS,
          new Object[]{aoi.getParentRowId(), aoi.getRowId(), aoi.getId(), aoi.getObjectClass()}));
    }
    executeStatement(session, batch);

    if (LOG.isDebugEnabled()) {
      LOG.debug("END saveAcl");
    }
  }

  @Override
  public void updateAcl(AclObjectIdentity aoi, List<AclEntry> entries) throws AclNotFoundException {
    assertAclObjectIdentity(aoi);

    Batch batch = QueryBuilder.batch();
    ResultSet aclTableRS =
        RetryableMethodExecutor.execute(ERROR_MASSAGE_IN_CASE_ALL_RETRY_FAILED,
            ACL_REPO_DEFAULT_MAX_ATTEMPTS,
            Retryable.DEFAULT_DELAY_BETWEEN_ATTEMPTS,
            () -> executeStatement(session, QueryBuilder
                .select()
                .all()
                .from(keyspace, ACL_TABLE)
                .where(QueryBuilder.eq(COMMON_ID_FIELD, aoi.getRowId()))));

    List<Row> recordsForDeletion = getRecordsForDeletion(entries, aclTableRS);
    getQueriesForDeletion(recordsForDeletion).forEach(batch::add);

    if (LOG.isDebugEnabled()) {
      LOG.debug("BEGIN updateAcl: aclObjectIdentity: " + aoi + ", entries: " + entries);
    }

    // Check this object identity is already persisted
    AclObjectIdentity persistedAoi = findAclObjectIdentity(aoi);
    if (persistedAoi == null) {
      throw new AclNotFoundException("Object identity '" + aoi + "' does not exist");
    }

    // Update AOI & delete existing ACLs
    batch.add(QueryBuilder.insertInto(keyspace, AOI_TABLE)
                          .values(AOI_KEYS,
                              new Object[]{aoi.getRowId(), aoi.getId(), aoi.getObjectClass(), aoi.isEntriesInheriting(),
                                  aoi.getOwnerId(), aoi.isOwnerPrincipal(), aoi.getParentObjectId(),
                                  aoi.getParentObjectClass()}));
    // Check if parent is different and delete from children table
    boolean parentChanged = false;
    if (!(persistedAoi.getParentRowId() == null ? (aoi.getParentRowId() == null)
        : persistedAoi.getParentRowId().equals(aoi.getParentRowId()))) {
      parentChanged = true;

      if (persistedAoi.getParentRowId() != null) {
        batch.add(QueryBuilder.delete().all().from(keyspace, CHILDREN_TABLE)
                              .where(QueryBuilder.eq(COMMON_ID_FIELD, persistedAoi.getParentRowId()))
                              .and(QueryBuilder.eq(CHILDREN_TABLE_CHILD_ID_FIELD, aoi.getRowId())));
      }
    }
    if (entries != null && !entries.isEmpty()) {
      for (AclEntry entry : entries) {
        batch.add(QueryBuilder.insertInto(keyspace, ACL_TABLE).values(ACL_KEYS,
            new Object[]{aoi.getRowId(), entry.getOrder(), entry.getSid(), entry.getMask(),
                entry.isSidPrincipal(), entry.isGranting(), entry.isAuditSuccess(),
                entry.isAuditFailure()}));
      }
    }
    if (parentChanged && (aoi.getParentRowId() != null)) {
        batch.add(QueryBuilder.insertInto(keyspace, CHILDREN_TABLE).values(CHILD_KEYS,
            new Object[]{aoi.getParentRowId(), aoi.getRowId(), aoi.getId(), aoi.getObjectClass()}));
      
    }
    RetryableMethodExecutor.execute(ERROR_MASSAGE_IN_CASE_ALL_RETRY_FAILED,
        ACL_REPO_DEFAULT_MAX_ATTEMPTS,
        Retryable.DEFAULT_DELAY_BETWEEN_ATTEMPTS,
        () -> executeStatement(session, batch));

    if (LOG.isDebugEnabled()) {
      LOG.debug("END updateAcl");
    }
  }

  private List<Row> getRecordsForDeletion(List<AclEntry> entries, ResultSet aclTableRS) {
    List<Row> recordsForDeletion = new ArrayList<>();
    if (entries != null) {
      aclTableRS.forEach(aclTableRow -> {
        Optional<AclEntry> optionalEntry = entries
            .stream()
            .filter(entry -> aclTableRow.getString(COMMON_ID_FIELD).equals(entry.getId())
                && aclTableRow.getString(ACL_TABLE_SID_FIELD).equals(entry.getSid())
                && aclTableRow.getInt(ACL_TABLE_ACL_ORDER_FIELD) == (entry.getOrder())
            )
            .findFirst();
        if (optionalEntry.isEmpty()) {
          recordsForDeletion.add(aclTableRow);
        }
      });
      return recordsForDeletion;
    } else {
      return aclTableRS.all();
    }
  }

  private List<Delete.Where> getQueriesForDeletion(List<Row> recordsForDeletion) {
    List<Delete.Where> deleteQueries = new ArrayList<>();
    recordsForDeletion.forEach(row -> deleteQueries.add(
        QueryBuilder
            .delete()
            .from(keyspace, ACL_TABLE)
            .where(QueryBuilder.eq(COMMON_ID_FIELD, row.getString(COMMON_ID_FIELD)))
            .and(QueryBuilder.eq(ACL_TABLE_SID_FIELD, row.getString(ACL_TABLE_SID_FIELD)))
            .and(QueryBuilder.eq(ACL_TABLE_ACL_ORDER_FIELD, row.getInt(ACL_TABLE_ACL_ORDER_FIELD)))
    ));
    return deleteQueries;
  }


  /**
   * Validates all <code>AclObjectIdentity</code> objects in the list.
   *
   * @param aoiList a list of <code>AclObjectIdentity</code> objects to validate.
   */
  private void assertAclObjectIdentityList(List<AclObjectIdentity> aoiList) {
    Assert.notEmpty(aoiList, "The AclObjectIdentity list cannot be empty");
    for (AclObjectIdentity aoi : aoiList) {
      assertAclObjectIdentity(aoi);
    }
  }

  /**
   * Validates an <code>AclObjectIdentity</code> object.
   *
   * @param aoi the <code>AclObjectIdentity</code> object to validate.
   */
  private void assertAclObjectIdentity(AclObjectIdentity aoi) {
    Assert.notNull(aoi, "The AclObjectIdentity cannot be null");
    Assert.notNull(aoi.getId(), "The AclObjectIdentity id cannot be null");
    Assert.notNull(aoi.getObjectClass(), "The AclObjectIdentity objectClass cannot be null");
  }

  /**
   * Converts a <code>AOI table row</code> from a Cassandra result to an
   * <code>AclObjectIdentity</code> object.
   *
   * @param row the <code>AOI table row</code> representing an
   * <code>AclObjectIdentity</code>.
   * @return a full <code>AclObjectIdentity</code> object with the values retrieved from Cassandra.
   */
  private AclObjectIdentity convertToFullAclObjectIdentity(Row row) {
    AclObjectIdentity result = null;
    if (row != null) {
      result = convertToSimpleAclObjectIdentity(row);
      result.setOwnerId(row.getString("owner"));
      result.setEntriesInheriting(row.getBool("isInheriting"));
      result.setOwnerPrincipal(row.getBool("isOwnerPrincipal"));
      result.setParentObjectClass(row.getString("parentObjClass"));
      result.setParentObjectId(row.getString("parentObjId"));
    }
    return result;
  }
  /**
   * Converts a <code>child or AOI table row</code> from a Cassandra result to a simple
   * <code>AclObjectIdentity</code> object. By simple, it means that the result object
   * will contain only the <code>id</code> and <code>objectClass</code>
   *
   * @param row the <code>child or AOI table row</code> representing an
   * <code>AclObjectIdentity</code>.
   * @return a simple <code>AclObjectIdentity</code> object with the values retrieved from Cassandra.
   */
  private AclObjectIdentity convertToSimpleAclObjectIdentity(Row row) {
    AclObjectIdentity result = null;
    if (row != null) {
      result = new AclObjectIdentity();
      result.setId(row.getString(COMMON_OBJ_ID_FIELD));
      result.setObjectClass(row.getString(COMMON_OBJ_CLASS_FIELD));
    }
    return result;
  }

  /**
   * Creates the schema for the table holding <code>AclObjectIdentity</code> representations.
   */
  @Retryable(maxAttempts = ACL_REPO_DEFAULT_MAX_ATTEMPTS)
  public final void createAoisTable() {
    try {
      executeStatement(session, createAoisTable);
    } catch (AlreadyExistsException e) {
      LOG.warn(e.getMessage(), e);
    }
  }

  /**
   * Creates the schema for the table holding <code>AclObjectIdentity</code> children.
   */
  @Retryable(maxAttempts = ACL_REPO_DEFAULT_MAX_ATTEMPTS)
  public final void createChildrenTable() {
    try {
      executeStatement(session, createChildrenTable);
    } catch (AlreadyExistsException e) {
      LOG.warn(e.getMessage(), e);
    }
  }

  /**
   * Creates the schema for the table holding <code>AclEntry</code> representations.
   */
  @Retryable(maxAttempts = ACL_REPO_DEFAULT_MAX_ATTEMPTS)
  public final void createAclsTable() {
    try {
      executeStatement(session, createAclsTable);
    } catch (AlreadyExistsException e) {
      LOG.warn(e.getMessage(), e);
    }
  }

}
