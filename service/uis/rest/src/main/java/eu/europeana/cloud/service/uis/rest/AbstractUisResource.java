package eu.europeana.cloud.service.uis.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.acls.domain.BasePermission;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;

public class AbstractUisResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractUisResource.class);

    @Autowired
    protected MutableAclService mutableAclService;

    protected static final int DEFAULT_RETRIES = 3;
    protected static final int SLEEP_TIME = 5000;


    protected MutableAcl getAcl(String creatorName, ObjectIdentity cloudIdIdentity) {
        MutableAcl acl = mutableAclService.createAcl(cloudIdIdentity);
        acl.insertAce(0, BasePermission.READ, new PrincipalSid(creatorName), true);
        acl.insertAce(1, BasePermission.WRITE, new PrincipalSid(creatorName), true);
        acl.insertAce(2, BasePermission.DELETE, new PrincipalSid(creatorName), true);
        acl.insertAce(3, BasePermission.ADMINISTRATION, new PrincipalSid(creatorName), true);
        return acl;
    }

    protected void updateAcl(MutableAcl providerAcl) {
        int retries = DEFAULT_RETRIES;
        while (true) {
            try {
                mutableAclService.updateAcl(providerAcl);
                break;
            } catch (Exception e) {
                if (retries-- > 0) {
                    waitForSpecificTime();
                } else {
                    LOGGER.error("Error while updating ACLs for cloudId creation. Exception: {}", e.getMessage());
                    throw e;
                }
            }
        }
    }

    protected void waitForSpecificTime() {
        try {
            Thread.sleep(SLEEP_TIME);
        } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
            LOGGER.error(e1.getMessage());
        }
    }
}
