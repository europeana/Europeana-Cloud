package eu.europeana.aas.acl.repository;

import eu.europeana.aas.acl.model.AclEntry;
import eu.europeana.aas.acl.model.AclObjectIdentity;
import eu.europeana.cloud.common.model.Role;

import static org.junit.Assert.assertEquals;

class AclUtils {

    //Needed to ensure that creating instance of Utils class is blocked
    private AclUtils() {
    }

    protected static final String sid1 = "sid1@system";
    protected static final String aoi_id = "123";
    protected static final String aoi_parent_id = "456";
    protected static final String aoi_class = "a.b.c.Class";
    protected static final String ROLE_ADMIN = Role.ADMIN;


    protected static AclEntry createTestAclEntry(String sid, int order) {
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

    protected static AclObjectIdentity createTestAclObjectIdentity() {
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

    protected static void assertAclObjectIdentity(AclObjectIdentity expected,
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

    protected static void assertAclEntry(AclObjectIdentity expectedOi,
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
