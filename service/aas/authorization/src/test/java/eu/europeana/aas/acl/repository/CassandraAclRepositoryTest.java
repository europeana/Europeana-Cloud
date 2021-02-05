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

import eu.europeana.aas.acl.CassandraTestBase;
import eu.europeana.aas.acl.TestContextConfiguration;
import eu.europeana.aas.acl.model.AclEntry;
import eu.europeana.aas.acl.model.AclObjectIdentity;
import eu.europeana.aas.acl.repository.exceptions.AclAlreadyExistsException;
import eu.europeana.aas.acl.repository.exceptions.AclNotFoundException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.*;

import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = TestContextConfiguration.class)
public class CassandraAclRepositoryTest extends CassandraTestBase {

    private static final String sid1 = "sid1@system";
    private static final String aoi_id = "123";
    private static final String aoi_parent_id = "456";
    private static final String aoi_class = "a.b.c.Class";
    private static final String ROLE_ADMIN = "ROLE_ADMIN";

    @Autowired
    private CassandraAclRepository service;

    @Before
    public void setUp() throws Exception {

	service.createAoisTable();
	service.createAclsTable();
	service.createChilrenTable();

	SecurityContextHolder
		.getContext()
		.setAuthentication(
			new UsernamePasswordAuthenticationToken(
				sid1,
				"password",
				Arrays.asList(new SimpleGrantedAuthority[] { new SimpleGrantedAuthority(
					ROLE_ADMIN) })));
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testSaveFindUpdateDeleteAcl() {
	AclObjectIdentity newAoi = createDefaultTestAOI();

	service.saveAcl(newAoi);

	AclObjectIdentity aoi = service.findAclObjectIdentity(newAoi);
	assertAclObjectIdentity(newAoi, aoi);

	aoi.setEntriesInheriting(false);
	// Do not fill in id. It should get values automatically anyway.
	AclEntry entry1 = createTestAclEntry(sid1, 0);
	AclEntry entry2 = createTestAclEntry(ROLE_ADMIN, 1);

	service.updateAcl(aoi, Arrays.asList(new AclEntry[] { entry1, entry2 }));

	Map<AclObjectIdentity, Set<AclEntry>> result = service.findAcls(Arrays
		.asList(new AclObjectIdentity[] { aoi }));
	assertEquals(1, result.size());
	assertAclObjectIdentity(aoi, result.keySet().iterator().next());
	Set<AclEntry> aclEntries = result.values().iterator().next();
	Iterator<AclEntry> it = aclEntries.iterator();
	assertAclEntry(aoi, entry1, it.next());
	assertAclEntry(aoi, entry2, it.next());

	service.deleteAcls(Arrays.asList(new AclObjectIdentity[] { aoi }));

	aoi = service.findAclObjectIdentity(aoi);
	assertNull(aoi);
    }

    @Test
    public void testFindAclListManyAcls() {
	AclObjectIdentity newAoi1 = createDefaultTestAOI();
	AclObjectIdentity newAoi2 = createDefaultTestAOI();
	newAoi2.setId("567");

	AclEntry entry1 = createTestAclEntry(sid1, 0);

	service.saveAcl(newAoi1);
	service.saveAcl(newAoi2);
	service.updateAcl(newAoi1, Arrays.asList(new AclEntry[] { entry1 }));
	service.updateAcl(newAoi2, Arrays.asList(new AclEntry[] { entry1 }));
	Map<AclObjectIdentity, Set<AclEntry>> result = service.findAcls(Arrays
		.asList(new AclObjectIdentity[] { newAoi1, newAoi2 }));

	assertEquals(2, result.size());
	Iterator<AclObjectIdentity> it = result.keySet().iterator();
	AclObjectIdentity resAoi = it.next();
	if (resAoi.getId().equals(newAoi1.getId())) {
	    assertAclObjectIdentity(newAoi1, resAoi);
	    assertAclObjectIdentity(newAoi2, it.next());
	} else {
	    assertAclObjectIdentity(newAoi2, resAoi);
	    assertAclObjectIdentity(newAoi1, it.next());
	}

	Iterator<Set<AclEntry>> it2 = result.values().iterator();
	Set<AclEntry> aclEntries = it2.next();
	assertEquals(1, aclEntries.size());
	AclEntry resEntry = aclEntries.iterator().next();
	if (resEntry.getId().startsWith(newAoi2.getRowId())) {
	    assertAclEntry(newAoi2, entry1, resEntry);
	    aclEntries = it2.next();
	    assertEquals(1, aclEntries.size());
	    assertAclEntry(newAoi1, entry1, aclEntries.iterator().next());
	} else {
	    assertAclEntry(newAoi1, entry1, resEntry);
	    aclEntries = it2.next();
	    assertEquals(1, aclEntries.size());
	    assertAclEntry(newAoi2, entry1, aclEntries.iterator().next());
	}
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFindAclListEmpty() {
	service.findAcls(new ArrayList<AclObjectIdentity>());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFindNullAclList() {
	service.findAcls(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFindNullAcl() {
	service.findAclObjectIdentity(null);
    }

    @Test
    public void testFindAclNotExisting() {
	AclObjectIdentity newAoi = new AclObjectIdentity();
	newAoi.setId("invalid");
	newAoi.setObjectClass(aoi_class);
	newAoi.setOwnerId(sid1);
	service.findAclObjectIdentity(newAoi);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFindAclWithNullValues() {
	AclObjectIdentity newAoi = new AclObjectIdentity();
	service.findAclObjectIdentity(newAoi);
    }

    @Test
    public void testFindAclChildren() {
	AclObjectIdentity newAoi1 = createDefaultTestAOI();
	service.saveAcl(newAoi1);

	AclObjectIdentity newAoi2 = createDefaultTestAOI();
	newAoi2.setId("456");
	newAoi2.setParentObjectClass(newAoi1.getObjectClass());
	newAoi2.setParentObjectId(newAoi1.getId());
	service.saveAcl(newAoi2);

	List<AclObjectIdentity> children = service
		.findAclObjectIdentityChildren(newAoi1);
	assertNotNull(children);
	assertEquals(1, children.size());
	assertEquals(newAoi2.getId(), children.get(0).getId());
	assertEquals(newAoi2.getObjectClass(), children.get(0).getObjectClass());
    }

    @Test
    public void testFindAclChildrenForAclWithNoChildren() {
	AclObjectIdentity newAoi1 = createDefaultTestAOI();
	service.saveAcl(newAoi1);
	List<AclObjectIdentity> children = service
		.findAclObjectIdentityChildren(newAoi1);
	assertTrue(children.isEmpty());
    }

    @Test
    public void testFindAclChildrenForNotExistingAcl() {
	AclObjectIdentity newAoi = new AclObjectIdentity();
	newAoi.setId("invalid");
	newAoi.setObjectClass(aoi_class);
	newAoi.setOwnerId(sid1);
	List<AclObjectIdentity> children = service
		.findAclObjectIdentityChildren(newAoi);
	assertTrue(children.isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFindNullAclChildren() {
	service.findAclObjectIdentityChildren(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFindAclChildrenWithNullValues() {
	AclObjectIdentity newAoi = new AclObjectIdentity();
	service.findAclObjectIdentityChildren(newAoi);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUpdateNullAcl() {
	service.updateAcl(null, null);
    }

    @Test
    public void testUpdateAclNullEntries() {
	AclObjectIdentity newAoi = createDefaultTestAOI();
	service.saveAcl(newAoi);

	AclEntry entry1 = createTestAclEntry(sid1, 0);
	service.updateAcl(newAoi, Arrays.asList(new AclEntry[] { entry1 }));

	Map<AclObjectIdentity, Set<AclEntry>> result = service.findAcls(Arrays
		.asList(new AclObjectIdentity[] { newAoi }));
	assertEquals(1, result.size());
	assertAclObjectIdentity(newAoi, result.keySet().iterator().next());
	Set<AclEntry> aclEntries = result.values().iterator().next();
	assertAclEntry(newAoi, entry1, aclEntries.iterator().next());

	service.updateAcl(newAoi, null);
	result = service.findAcls(Arrays
		.asList(new AclObjectIdentity[] { newAoi }));
	assertEquals(1, result.size());
	assertAclObjectIdentity(newAoi, result.keySet().iterator().next());
	assertTrue(result.values().iterator().next().isEmpty());
    }

    @Test(expected = AclNotFoundException.class)
    public void testUpdateAclNotExisting() {
	AclObjectIdentity newAoi = new AclObjectIdentity();
	newAoi.setId("invalid");
	newAoi.setObjectClass(aoi_class);
	newAoi.setOwnerId(sid1);
	service.updateAcl(newAoi, new ArrayList<AclEntry>());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSaveNullAcl() {
	service.saveAcl(null);
    }

    @Test(expected = AclAlreadyExistsException.class)
    public void testSaveAclAlreadyExisting() {
	AclObjectIdentity newAoi = createDefaultTestAOI();
	service.saveAcl(newAoi);
	service.saveAcl(newAoi);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeleteNullAcl() {
	service.deleteAcls(null);
    }

    @Test
    public void testDeleteAclNotExisting() {
	AclObjectIdentity newAoi = new AclObjectIdentity();
	newAoi.setId("invalid");
	newAoi.setObjectClass(aoi_class);
	newAoi.setOwnerId(sid1);
	service.deleteAcls(Arrays.asList(new AclObjectIdentity[] { newAoi }));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeleteEmptyAclList() {
	service.deleteAcls(new ArrayList<AclObjectIdentity>());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSaveAclWithNullValues() {
	AclObjectIdentity newAoi = new AclObjectIdentity();
	service.saveAcl(newAoi);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDeleteAclWithNullValues() {
	AclObjectIdentity newAoi = new AclObjectIdentity();
	service.deleteAcls(Arrays.asList(new AclObjectIdentity[] { newAoi }));
    }

    private AclEntry createTestAclEntry(String sid, int order) {
	AclEntry entry1 = new AclEntry();
	entry1.setAuditFailure(true);
	entry1.setAuditSuccess(true);
	entry1.setGranting(true);
	entry1.setMask(1);
	entry1.setSid(sid);
	entry1.setOrder(order);
	if (sid.startsWith("ROLE_")) {
	    entry1.setSidPrincipal(false);
	} else {
	    entry1.setSidPrincipal(true);
	}
	return entry1;
    }

    private AclObjectIdentity createDefaultTestAOI() {
	AclObjectIdentity newAoi = new AclObjectIdentity();
	newAoi.setId(aoi_id);
	newAoi.setEntriesInheriting(true);
	newAoi.setObjectClass(aoi_class);
	newAoi.setOwnerId(sid1);
	newAoi.setOwnerPrincipal(true);
	newAoi.setParentObjectId(aoi_parent_id);
	newAoi.setParentObjectClass(aoi_class);
	return newAoi;
    }

    private void assertAclObjectIdentity(AclObjectIdentity expected,
	    AclObjectIdentity actual) {
	assertEquals(expected.getId(), actual.getId());
	assertEquals(expected.getObjectClass(), actual.getObjectClass());
	assertEquals(expected.getOwnerId(), actual.getOwnerId());
	assertEquals(expected.getParentObjectId(), actual.getParentObjectId());
	assertEquals(expected.getParentObjectClass(),
		actual.getParentObjectClass());
	assertEquals(expected.isEntriesInheriting(),
		actual.isEntriesInheriting());
	assertEquals(expected.isOwnerPrincipal(), actual.isOwnerPrincipal());
    }

    private void assertAclEntry(AclObjectIdentity expectedOi,
	    AclEntry expected, AclEntry actual) {
	assertEquals(expectedOi.getObjectClass() + ":" + expectedOi.getId()
		+ ":" + expected.getSid() + ":" + expected.getOrder(),
		actual.getId());
	assertEquals(expected.getMask(), actual.getMask());
	assertEquals(expected.getOrder(), actual.getOrder());
	assertEquals(expected.getSid(), actual.getSid());
	assertEquals(expected.isAuditFailure(), actual.isAuditFailure());
	assertEquals(expected.isAuditSuccess(), actual.isAuditSuccess());
	assertEquals(expected.isGranting(), actual.isGranting());
	assertEquals(expected.isSidPrincipal(), actual.isSidPrincipal());
    }
}
