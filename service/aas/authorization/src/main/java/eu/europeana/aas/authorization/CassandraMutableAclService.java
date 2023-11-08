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
package eu.europeana.aas.authorization;

import eu.europeana.aas.authorization.model.AclEntry;
import eu.europeana.aas.authorization.model.AclObjectIdentity;
import eu.europeana.aas.authorization.repository.AclRepository;
import eu.europeana.aas.authorization.repository.exceptions.AclNotFoundException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.security.acls.domain.AclAuthorizationStrategy;
import org.springframework.security.acls.domain.PermissionFactory;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.AclCache;
import org.springframework.security.acls.model.AlreadyExistsException;
import org.springframework.security.acls.model.ChildrenExistException;
import org.springframework.security.acls.model.MutableAcl;
import org.springframework.security.acls.model.NotFoundException;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.acls.model.PermissionGrantingStrategy;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.util.Assert;

/**
 * Provides support for creating and storing {@link Acl} instances in Cassandra, using the {@link AclRepository}.
 *
 * @author Rigas Grigoropoulos
 */
public class CassandraMutableAclService extends CassandraAclService implements ExtendedAclService {

  private static final Log LOG = LogFactory.getLog(CassandraMutableAclService.class);
  public static final String OBJECT_IDENTITY_REQUIRED_MESSAGE = "Object Identity required";

  /**
   * Constructs a new <code>CassandraMutableAclService</code> object.
   *
   * @param aclRepository the {@link AclRepository} to use for access to the database.
   * @param aclCache the {@link AclCache} to use (can be <code>null</code>).
   * @param grantingStrategy the {@link PermissionGrantingStrategy} to use when creating {@link Acl} objects.
   * @param aclAuthorizationStrategy the {@link AclAuthorizationStrategy} to use when creating {@link Acl} objects.
   * @param permissionFactory the {@link PermissionFactory} to use when creating {@link AccessControlEntry} objects.
   */
  public CassandraMutableAclService(AclRepository aclRepository, AclCache aclCache,
      PermissionGrantingStrategy grantingStrategy, AclAuthorizationStrategy aclAuthorizationStrategy,
      PermissionFactory permissionFactory) {
    super(aclRepository, aclCache, grantingStrategy, aclAuthorizationStrategy, permissionFactory);
  }

  @Override
  public MutableAcl createAcl(ObjectIdentity objectIdentity) throws AlreadyExistsException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("EXECUTE createAcl: objectIdentity: " + objectIdentity);
    }

    AclObjectIdentity newAoi = createAclObjectIdentity(objectIdentity);
    if (aclRepository.findAclObjectIdentity(newAoi) != null) {
      throw new AlreadyExistsException("Object identity '" + newAoi + "' already exists");
    }

    return saveAcl(objectIdentity);
  }

  public MutableAcl createOrUpdateAcl(ObjectIdentity objectIdentity) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("EXECUTE insertOrUpdateAcl: objectIdentity: " + objectIdentity);
    }

    AclObjectIdentity newAoi = createAclObjectIdentity(objectIdentity);
    AclObjectIdentity oldAoi = aclRepository.findAclObjectIdentity(newAoi);
    if (aoiIsOwnedByDifferentUser(oldAoi)) {
      throw new AlreadyExistsException("Object identity '" + newAoi + "' already exists and is owned by other user!");
    }

    return saveAcl(objectIdentity);
  }

  private AclObjectIdentity createAclObjectIdentity(ObjectIdentity objectIdentity) {
    // Need to retrieve the current principal, in order to know who "owns"
    // this ACL (can be changed later on)
    Assert.notNull(objectIdentity, OBJECT_IDENTITY_REQUIRED_MESSAGE);

    Authentication auth = SecurityContextHolder.getContext().getAuthentication();
    PrincipalSid sid = new PrincipalSid(auth);

    AclObjectIdentity newAoi = new AclObjectIdentity(objectIdentity);
    newAoi.setOwnerId(sid.getPrincipal());
    newAoi.setOwnerPrincipal(true);
    newAoi.setEntriesInheriting(false);
    return newAoi;
  }

  private MutableAcl saveAcl(ObjectIdentity objectIdentity) {
    AclObjectIdentity newAoi = createAclObjectIdentity(objectIdentity);
    if (LOG.isDebugEnabled()) {
      LOG.debug("START saveAcl: acl: {}" + objectIdentity);
    }
    aclRepository.saveAcl(newAoi);

    // Retrieve the ACL via superclass (ensures cache registration, proper retrieval etc)
    Acl acl = readAclById(objectIdentity);
    Assert.isInstanceOf(MutableAcl.class, acl, "MutableAcl should be been returned");

    if (LOG.isDebugEnabled()) {
      LOG.debug("END saveAcl: acl: " + acl);
    }

    return (MutableAcl) acl;
  }

  private boolean aoiIsOwnedByDifferentUser(AclObjectIdentity aoi) {
    if (aoi == null) {
      return false;
    }

    String currentUserId = SecurityContextHolder.getContext().getAuthentication().getName();
    return (currentUserId == null) || !currentUserId.equals(aoi.getOwnerId());
  }

  @Override
  public void deleteAcl(ObjectIdentity objectIdentity, boolean deleteChildren) throws ChildrenExistException {
    Assert.notNull(objectIdentity, OBJECT_IDENTITY_REQUIRED_MESSAGE);
    Assert.notNull(objectIdentity.getIdentifier(), "Object Identity doesn't provide an identifier");

    if (LOG.isDebugEnabled()) {
      LOG.debug("BEGIN deleteAcl: objectIdentity: " + objectIdentity + ", deleteChildren: " + deleteChildren);
    }

    List<ObjectIdentity> objectsToDelete = new ArrayList<>();
    objectsToDelete.add(objectIdentity);

    List<ObjectIdentity> children = findChildren(objectIdentity);
    if (deleteChildren) {
      for (ObjectIdentity child : children) {
        objectsToDelete.addAll(calculateChildrenReccursively(child));
      }
    } else if (children != null && !children.isEmpty()) {
      throw new ChildrenExistException("Cannot delete '" + objectIdentity + "' (has " + children.size()
          + " children)");
    }

    List<AclObjectIdentity> objIdsToDelete = new ArrayList<>(objectsToDelete.size());
    for (ObjectIdentity objId : objectsToDelete) {
      objIdsToDelete.add(new AclObjectIdentity(objId));
    }
    aclRepository.deleteAcls(objIdsToDelete);

    // Clear the cache
    if (aclCache != null) {
      for (ObjectIdentity obj : objectsToDelete) {
        aclCache.evictFromCache(obj);
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("END deleteAcl");
    }
  }

  @Override
  public MutableAcl updateAcl(MutableAcl acl) throws NotFoundException {
    Assert.notNull(acl, "MutableAcl required");
    Assert.notNull(acl.getObjectIdentity(), OBJECT_IDENTITY_REQUIRED_MESSAGE);
    Assert.notNull(acl.getObjectIdentity().getIdentifier(), "Object Identity doesn't provide an identifier");

    if (LOG.isDebugEnabled()) {
      LOG.debug("BEGIN updateAcl: acl: " + acl);
    }

    try {
      aclRepository.updateAcl(new AclObjectIdentity(acl), convertToAclEntries(acl));
    } catch (AclNotFoundException e) {
      throw new NotFoundException(e.getMessage(), e);
    }

    // Clear the cache, including children
    clearCacheIncludingChildren(acl.getObjectIdentity());

    // Retrieve the ACL via superclass (ensures cache registration, proper retrieval etc)
    MutableAcl result = (MutableAcl) readAclById(acl.getObjectIdentity());

    if (LOG.isDebugEnabled()) {
      LOG.debug("END updateAcl: acl: " + result);
    }
    return result;
  }

  /**
   * Finds the complete children hierarchy starting from the provided {@link ObjectIdentity}.
   *
   * @param rootChild the root {@link ObjectIdentity} to start looking for children.
   * @return a list of all child {@link ObjectIdentity} objects, including the provided root object.
   */
  private List<ObjectIdentity> calculateChildrenReccursively(ObjectIdentity rootChild) {
    List<ObjectIdentity> result = new ArrayList<>();
    result.add(rootChild);
    List<ObjectIdentity> children = findChildren(rootChild);
    if (children != null) {
      for (ObjectIdentity child : children) {
        result.addAll(calculateChildrenReccursively(child));
      }
    }
    return result;
  }

  /**
   * Converts an {@link Acl} to a list of {@link AclEntry} objects.
   *
   * @param acl the {@link Acl} to convert.
   * @return the list of derived {@link AclEntry} objects.
   */
  private List<AclEntry> convertToAclEntries(Acl acl) {
    List<AclEntry> result = new ArrayList<>(acl.getEntries().size());

    for (AccessControlEntry entry : acl.getEntries()) {
      result.add(new AclEntry(entry));
    }
    return result;
  }

  /**
   * Evicts the provided {@link ObjectIdentity} and the complete children hierarchy from the cache.
   *
   * @param objectIdentity the parent {@link ObjectIdentity} to evict.
   */
  private void clearCacheIncludingChildren(ObjectIdentity objectIdentity) {
    Assert.notNull(objectIdentity, "ObjectIdentity required");
    List<ObjectIdentity> children = findChildren(objectIdentity);
    if (children != null) {
      for (ObjectIdentity child : children) {
        clearCacheIncludingChildren(child);
      }
    }

    if (aclCache != null) {
      aclCache.evictFromCache(objectIdentity);
    }
  }

}
