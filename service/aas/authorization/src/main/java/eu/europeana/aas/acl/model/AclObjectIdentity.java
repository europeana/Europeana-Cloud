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
package eu.europeana.aas.acl.model;

import org.springframework.security.acls.domain.GrantedAuthoritySid;
import org.springframework.security.acls.domain.ObjectIdentityImpl;
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.Acl;
import org.springframework.security.acls.model.ObjectIdentity;
import org.springframework.security.acls.model.Sid;
import org.springframework.util.Assert;

/**
 * DTO representing the identity of an individual domain object instance.
 * 
 * @author Rigas Grigoropoulos
 *
 */
public class AclObjectIdentity {

	private String id;
	private String objectClass;
	private String parentObjectId;
	private String parentObjectClass;
	private String ownerId;
	private boolean ownerPrincipal;
	private boolean entriesInheriting;

	/**
	 * Constructs a new <code>AclObjectIdentity</code>
	 */
	public AclObjectIdentity() {}
	
	/**
	 * Constructs a new <code>AclObjectIdentity</code> out of the provided {@link ObjectIdentity}.
	 * 
	 * @param objectIdentity the {@link ObjectIdentity} to use for parameter population.
	 */
	public AclObjectIdentity(ObjectIdentity objectIdentity) {
		Assert.notNull(objectIdentity, "ObjectIdentity required");
		objectClass = objectIdentity.getType();
		id = (String) objectIdentity.getIdentifier();
	}
	
	/**
	 * Constructs a new <code>AclObjectIdentity</code> out of the provided {@link Acl}.
	 * 
	 * @param acl the {@link Acl} to use for parameter population.
	 */
	public AclObjectIdentity(Acl acl) {
		Assert.notNull(acl, "Acl required");		
		entriesInheriting = acl.isEntriesInheriting();
		id = (String) acl.getObjectIdentity().getIdentifier();
		objectClass = acl.getObjectIdentity().getType();
		
		if (acl.getOwner() instanceof PrincipalSid) {
			ownerId = ((PrincipalSid) acl.getOwner()).getPrincipal();
			ownerPrincipal = true;
		} else if (acl.getOwner() instanceof GrantedAuthoritySid) {
			ownerId = ((GrantedAuthoritySid) acl.getOwner()).getGrantedAuthority();
			ownerPrincipal = false;
		}
	
		parentObjectId = acl.getParentAcl() != null ? (String) acl.getParentAcl().getObjectIdentity().getIdentifier() : null;
		parentObjectClass = acl.getParentAcl() != null ? (String) acl.getParentAcl().getObjectIdentity().getType() : null;
	}
	
	/**
	 * @return the identifier of this <code>AclObjectIdentity</code>. 
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param id the identifier of this <code>AclObjectIdentity</code>. 
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @return the type of the domain object managed by this <code>AclObjectIdentity</code>.
	 */
	public String getObjectClass() {
		return objectClass;
	}

	/**
	 * @return true if the owner of this <code>AclObjectIdentity</code> is of type {@link PrincipalSid}
	 * 		of false if it is of type {@link GrantedAuthoritySid}. 
	 */
	public boolean isOwnerPrincipal() {
		return ownerPrincipal;
	}

	/**
	 * @param ownerPrincipal whether the owner of this <code>AclObjectIdentity</code> is of type {@link PrincipalSid}.
	 */
	public void setOwnerPrincipal(boolean ownerPrincipal) {
		this.ownerPrincipal = ownerPrincipal;
	}

	/**
	 * @param objectClass the type of the domain object managed by this <code>AclObjectIdentity</code>.
	 */
	public void setObjectClass(String objectClass) {
		this.objectClass = objectClass;
	}

	/**
	 * @return the identifier of the parent for this <code>AclObjectIdentity</code>. 
	 */
	public String getParentObjectId() {
		return parentObjectId;
	}

	/**
	 * @param parentObjectId the identifier of the parent for this <code>AclObjectIdentity</code>. 
	 */
	public void setParentObjectId(String parentObjectId) {
		this.parentObjectId = parentObjectId;
	}

	/**
	 * @return the identifier of the owner for this <code>AclObjectIdentity</code>.
	 */
	public String getOwnerId() {
		return ownerId;
	}
	
	/**
	 * @return the {@link Sid} object of the owner for this <code>AclObjectIdentity</code>.
	 */
	public Sid getOwnerSid() {
		Sid result = null;
		if (ownerPrincipal) {
			result = new PrincipalSid(ownerId);
		} else {
			result = new GrantedAuthoritySid(ownerId);
		}
		return result;
	}

	/**
	 * @param ownerId the identifier of the owner for this <code>AclObjectIdentity</code>.
	 */
	public void setOwnerId(String ownerId) {
		this.ownerId = ownerId;
	}

	/**
	 * @return true if parent entries inherit into the current <code>AclObjectIdentity</code>.
	 */
	public boolean isEntriesInheriting() {
		return entriesInheriting;
	}

	/**
	 * @param entriesInheriting true if parent entries inherit into the current <code>AclObjectIdentity</code>.
	 */
	public void setEntriesInheriting(boolean entriesInheriting) {
		this.entriesInheriting = entriesInheriting;
	}
	
	/**
	 * @return the type of the domain object of the parent of this <code>AclObjectIdentity</code>.
	 */
	public String getParentObjectClass() {
		return parentObjectClass;
	}
	
	/**
	 * @return the {@link ObjectIdentity} for the parent of this <code>AclObjectIdentity</code>.
	 */
	public ObjectIdentity getParentObjectIdentity() {
		if (parentObjectClass != null && parentObjectId != null) {
			return new ObjectIdentityImpl(parentObjectClass, parentObjectId);
		}
		return null;
	}

	/**
	 * @param parentObjectClass the type of the domain object of the parent of this <code>AclObjectIdentity</code>.
	 */
	public void setParentObjectClass(String parentObjectClass) {
		this.parentObjectClass = parentObjectClass;
	}

	/**
	 * @return the {@link ObjectIdentity} for this <code>AclObjectIdentity</code>.
	 */
	public ObjectIdentity toObjectIdentity() {
		return new ObjectIdentityImpl(objectClass, id);
	}
	
	/**
	 * @return the primary key under which this <code>AclObjectIdentity</code> is stored in the database.
	 */
	public String getRowId() {
		return objectClass + ":" + id;
	}
	
	/**
	 * @return the primary key under which the parent of this <code>AclObjectIdentity</code> is stored in the database.
	 */
	public String getParentRowId() {
		if (parentObjectId != null && parentObjectClass != null) {
			return parentObjectClass + ":" + parentObjectId;
		}
		return null;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("AclObjectIdentity [").append("id: ").append(id);
		sb.append(", objectClass: ").append(objectClass);
		sb.append(", parentObjectId: ").append(parentObjectId);
		sb.append(", parentObjectClass: ").append(parentObjectClass);
		sb.append(", ownerId: ").append(ownerId);
		sb.append(", ownerPrincipal: ").append(ownerPrincipal);
		sb.append(", entriesInheriting: ").append(entriesInheriting).append("]");
		return sb.toString();
	}

}
