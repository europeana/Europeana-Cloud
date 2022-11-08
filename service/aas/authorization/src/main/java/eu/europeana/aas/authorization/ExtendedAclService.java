package eu.europeana.aas.authorization;

import org.springframework.security.acls.model.AlreadyExistsException;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.MutableAclService;
import org.springframework.security.acls.model.ObjectIdentity;

/**
 * Extends original interface to allow idempotent writes.
 */
public interface ExtendedAclService extends MutableAclService {

    /**
     * Creates an empty <code>Acl</code> object in the database.
     * Method do not throw exception if object already exists as long as it is owned by current user.
     * @param objectIdentity the object identity to create
     *
     * @return an ACL object with its ID set
     *
     * @throws AlreadyExistsException if the passed object identity already has a record that belongs to other user
     */
    MutableAcl createOrUpdateAcl(ObjectIdentity objectIdentity);
}
