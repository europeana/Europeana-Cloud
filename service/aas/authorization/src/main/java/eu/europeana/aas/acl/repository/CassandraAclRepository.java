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
package eu.europeana.aas.acl.repository;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import eu.europeana.aas.acl.model.AclEntry;
import eu.europeana.aas.acl.model.AclObjectIdentity;
import eu.europeana.aas.acl.repository.exceptions.AclNotFoundException;
import eu.europeana.cloud.cassandra.CassandraConnectionProvider;
import eu.europeana.cloud.common.annotation.Retryable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.Assert;

import java.util.*;
import java.util.Map.Entry;

/**
 * Implementation of <code>AclRepository</code> using the DataStax Java Driver.
 * 
 * @author Rigas Grigoropoulos
 * @author Markus.Muhr@theeuropeanlibrary.org
 */
@Retryable
public final class CassandraAclRepository implements AclRepository {

	private static final Log LOG = LogFactory
	    .getLog(CassandraAclRepository.class);

    private static final String AOI_TABLE = "aois";
    private static final String CHILDREN_TABLE = "children";
    private static final String ACL_TABLE = "acls";

    private static final String[] AOI_KEYS = new String[] { "id", "objId",
	    "objClass", "isInheriting", "owner", "isOwnerPrincipal",
	    "parentObjId", "parentObjClass" };
    private static final String[] CHILD_KEYS = new String[] { "id", "childId",
	    "objId", "objClass" };
    private static final String[] ACL_KEYS = new String[] { "id", "aclOrder",
	    "sid", "mask", "isSidPrincipal", "isGranting", "isAuditSuccess",
	    "isAuditFailure" };
	private Statement createChildrenTable;
	private Statement createAoisTable;
	private Statement createAclsTable;

	private final Session session;
    private final String keyspace;



	/**
     * Constructs a new <code>CassandraAclRepositoryImpl</code>.
     * 
     * @param provider
     *            providing the <code>Session</code> to use for connectivity
     *            with Cassandra.
     * @param initSchema
     *            whether the keyspace and schema for storing ACLs should // *
     *            be created.
     */
	public CassandraAclRepository(CassandraConnectionProvider provider, boolean initSchema) {
		this(provider.getSession(), provider.getKeyspaceName(), initSchema);
	}

    /**
     * Constructs a new <code>CassandraAclRepositoryImpl</code>.
     * 
     * @param session
     *            the <code>Session</code> to use for connectivity with
     *            Cassandra.
     * @param keyspace
     *            whether the keyspace and schema for storing ACLs should be
     *            created.
     */
	public CassandraAclRepository(Session session, String keyspace) {
		this.session = session;
		this.keyspace = keyspace;
		initStatements(session);
	}

    /**
     * Constructs a new <code>CassandraAclRepositoryImpl</code> and optionally
     * creates the Cassandra keyspace and schema for storing ACLs.
     * 
     * @param session
     *            the <code>Session</code> to use for connectivity with
     *            Cassandra.
     * @param keyspace
     *            whether the keyspace and schema for storing ACLs should be
     *            created.
     * @param initSchema
     *            whether the keyspace and schema for storing ACLs should be
     *            created.
     */
	public CassandraAclRepository(Session session, String keyspace, boolean initSchema) {
		this(session, keyspace);
		if (initSchema) {
			createAoisTable();
			createChilrenTable();
			createAclsTable();
		}
	}


	private void initStatements(Session session){
		createChildrenTable = new SimpleStatement("CREATE TABLE children (" + "id varchar," + "childId varchar," + "objId varchar,"
				+ "objClass varchar," + "PRIMARY KEY (id, childId)" + ");");
		createAoisTable = new SimpleStatement("CREATE TABLE aois (" + "id varchar PRIMARY KEY," + "objId varchar," + "objClass varchar,"
				+ "isInheriting boolean," + "owner varchar," + "isOwnerPrincipal boolean," + "parentObjId varchar,"
				+ "parentObjClass varchar" + ");");
		createAclsTable = new SimpleStatement("CREATE TABLE acls (" + "id varchar," + "sid varchar," + "aclOrder int," + "mask int,"
				+ "isSidPrincipal boolean," + "isGranting boolean," + "isAuditSuccess boolean,"
				+ "isAuditFailure boolean," + "PRIMARY KEY (id, sid, aclOrder)" + ");");
	}



	@Retryable
	private ResultSet executeStatement(Session session, Statement statement){
		return session.execute(statement);
	}

	@Override
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
            resultMap.put(convertToAclObjectIdentity(row, true), new TreeSet<>(new Comparator<AclEntry>() {

                @Override
                public int compare(AclEntry o1, AclEntry o2) {
                    return  Integer.compare(o1.getOrder(), o2.getOrder());
                }
            }));
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
	public AclObjectIdentity findAclObjectIdentity(AclObjectIdentity objectId) {
		assertAclObjectIdentity(objectId);

		if (LOG.isDebugEnabled()) {
			LOG.debug("BEGIN findAclObjectIdentity: objectIdentity: " + objectId);
		}

		Row row = executeStatement(session,
				QueryBuilder.select().all().from(keyspace, AOI_TABLE)
						.where(QueryBuilder.eq("id", objectId.getRowId())))
				.one();
		AclObjectIdentity objectIdentity = convertToAclObjectIdentity(row, true);

		if (LOG.isDebugEnabled()) {
			LOG.debug("END findAclObjectIdentity: objectIdentity: " + objectIdentity);
		}
		return objectIdentity;
	}

    @Override
    public List<AclObjectIdentity> findAclObjectIdentityChildren(AclObjectIdentity objectId) {
        assertAclObjectIdentity(objectId);

        if (LOG.isDebugEnabled()) {
            LOG.debug("BEGIN findAclObjectIdentityChildren: objectIdentity: " + objectId);
        }

        ResultSet resultSet = executeStatement(session,QueryBuilder.select().all().from(keyspace, CHILDREN_TABLE)
                .where(QueryBuilder.eq("id", objectId.getRowId())));

        List<AclObjectIdentity> result = new ArrayList<>();
        for (Row row : resultSet.all()) {
            result.add(convertToAclObjectIdentity(row, false));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("END findAclObjectIdentityChildren: children: " + result);
        }
        return result;
    }

    @Override
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
	public void saveAcl(AclObjectIdentity aoi) {
		assertAclObjectIdentity(aoi);
		if (LOG.isDebugEnabled()) {
			LOG.debug("BEGIN saveAcl: aclObjectIdentity: " + aoi);
		}

		Batch batch = QueryBuilder.batch();
		batch.add(QueryBuilder.insertInto(keyspace, AOI_TABLE).values(AOI_KEYS,
				new Object[] { aoi.getRowId(), aoi.getId(), aoi.getObjectClass(), aoi.isEntriesInheriting(),
						aoi.getOwnerId(), aoi.isOwnerPrincipal(), aoi.getParentObjectId(),
						aoi.getParentObjectClass() }));

		if (aoi.getParentRowId() != null) {
			batch.add(QueryBuilder.insertInto(keyspace, CHILDREN_TABLE).values(CHILD_KEYS,
					new Object[] { aoi.getParentRowId(), aoi.getRowId(), aoi.getId(), aoi.getObjectClass() }));
		}
		executeStatement(session, batch);

		if (LOG.isDebugEnabled()) {
			LOG.debug("END saveAcl");
		}
	}

	@Override
	public void updateAcl(AclObjectIdentity aoi, List<AclEntry> entries) throws AclNotFoundException {
		assertAclObjectIdentity(aoi);

		if (LOG.isDebugEnabled()) {
			LOG.debug("BEGIN updateAcl: aclObjectIdentity: " + aoi + ", entries: " + entries);
		}

		// Check this object identity is already persisted
		AclObjectIdentity persistedAoi = findAclObjectIdentity(aoi);
		if (persistedAoi == null) {
			throw new AclNotFoundException("Object identity '" + aoi + "' does not exist");
		}

		// Update AOI & delete existing ACLs
		Batch batch = QueryBuilder.batch();
		batch.add(QueryBuilder.insertInto(keyspace, AOI_TABLE).values(AOI_KEYS,
				new Object[] { aoi.getRowId(), aoi.getId(), aoi.getObjectClass(), aoi.isEntriesInheriting(),
						aoi.getOwnerId(), aoi.isOwnerPrincipal(), aoi.getParentObjectId(),
						aoi.getParentObjectClass() }));
		batch.add(QueryBuilder.delete().all().from(keyspace, ACL_TABLE).where(QueryBuilder.eq("id", aoi.getRowId())));
		// Check if parent is different and delete from children table
		boolean parentChanged = false;
		if (!(persistedAoi.getParentRowId() == null ? aoi.getParentRowId() == null
				: persistedAoi.getParentRowId().equals(aoi.getParentRowId()))) {
			parentChanged = true;

			if (persistedAoi.getParentRowId() != null) {
				batch.add(QueryBuilder.delete().all().from(keyspace, CHILDREN_TABLE)
						.where(QueryBuilder.eq("id", persistedAoi.getParentRowId()))
						.and(QueryBuilder.eq("childId", aoi.getRowId())));
			}
		}
		executeStatement(session, batch);

		// Update ACLs & children table
		batch = QueryBuilder.batch();
		boolean executeBatch = false;

		if (entries != null && !entries.isEmpty()) {
			for (AclEntry entry : entries) {
				batch.add(QueryBuilder.insertInto(keyspace, ACL_TABLE).values(ACL_KEYS,
						new Object[] { aoi.getRowId(), entry.getOrder(), entry.getSid(), entry.getMask(),
								entry.isSidPrincipal(), entry.isGranting(), entry.isAuditSuccess(),
								entry.isAuditFailure() }));
			}
			executeBatch = true;
		}
		if (parentChanged) {
			if (aoi.getParentRowId() != null) {
				batch.add(QueryBuilder.insertInto(keyspace, CHILDREN_TABLE).values(CHILD_KEYS,
						new Object[] { aoi.getParentRowId(), aoi.getRowId(), aoi.getId(), aoi.getObjectClass() }));
			}
			executeBatch = true;
		}
		if (executeBatch) {
			executeStatement(session, batch);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("END updateAcl");
		}
	}

    /**
     * Validates all <code>AclObjectIdentity</code> objects in the list.
     * 
     * @param aoiList
     *            a list of <code>AclObjectIdentity</code> objects to validate.
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
     * @param aoi
     *            the <code>AclObjectIdentity</code> object to validate.
     */
	private void assertAclObjectIdentity(AclObjectIdentity aoi) {
		Assert.notNull(aoi, "The AclObjectIdentity cannot be null");
		Assert.notNull(aoi.getId(), "The AclObjectIdentity id cannot be null");
		Assert.notNull(aoi.getObjectClass(), "The AclObjectIdentity objectClass cannot be null");
	}

    /**
     * Converts a <code>Row</code> from a Cassandra result to an
     * <code>AclObjectIdentity</code> object.
     * 
     * @param row
     *            the <code>Row</code> representing an
     *            <code>AclObjectIdentity</code>.
     * @param fullObject
     *            whether the returned <code>AclObjectIdentity</code> object
     *            will contain only identification parameters or will be fully
     *            populated.
     * @return an <code>AclObjectIdentity</code> object with the values
     *         retrieved from Cassandra.
     */
	private AclObjectIdentity convertToAclObjectIdentity(Row row, boolean fullObject) {
		AclObjectIdentity result = null;
		if (row != null) {
			result = new AclObjectIdentity();
			result.setId(row.getString("objId"));
			result.setObjectClass(row.getString("objClass"));
			if (fullObject) {
				result.setOwnerId(row.getString("owner"));
				result.setEntriesInheriting(row.getBool("isInheriting"));
				result.setOwnerPrincipal(row.getBool("isOwnerPrincipal"));
				result.setParentObjectClass(row.getString("parentObjClass"));
				result.setParentObjectId(row.getString("parentObjId"));
			}
		}
		return result;
	}

    /**
     * Creates the schema for the table holding <code>AclObjectIdentity</code>
     * representations.
     */
	public void createAoisTable() {
		try {
			executeStatement(session, createAoisTable);
		} catch (AlreadyExistsException e) {
			LOG.warn(e.getMessage(), e);
		}
	}

    /**
     * Creates the schema for the table holding <code>AclObjectIdentity</code>
     * children.
     */
	public void createChilrenTable() {
		try {
			executeStatement(session, createChildrenTable);
		} catch (AlreadyExistsException e) {
			LOG.warn(e.getMessage(), e);
		}
	}

    /**
     * Creates the schema for the table holding <code>AclEntry</code>
     * representations.
     */
	public void createAclsTable() {
		try {
			executeStatement(session, createAclsTable);
		} catch (AlreadyExistsException e) {
			LOG.warn(e.getMessage(), e);
		}
	}

}
