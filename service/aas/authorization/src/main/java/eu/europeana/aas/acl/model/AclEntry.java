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
import org.springframework.security.acls.domain.PrincipalSid;
import org.springframework.security.acls.model.AccessControlEntry;
import org.springframework.security.acls.model.AuditableAccessControlEntry;
import org.springframework.security.acls.model.Sid;

/**
 * DTO representing an individual permission assignment.
 * 
 * @author Rigas Grigoropoulos
 *
 */
public class AclEntry {

	// id pattern: objectClass:objectId:sid:order
	private String id;
	private String sid;
	private boolean sidPrincipal;
	private int order;
	private int mask;
	private boolean granting;
	private boolean auditSuccess;
	private boolean auditFailure;

	/**
	 * Constructs a new <code>AclEntry</code>.
	 */
	public AclEntry() {}
	
	/**
	 * Constructs a new <code>AclEntry</code> out of the provided <code>AccessControlEntry</code>.
	 * 
	 * @param ace the {@link AccessControlEntry} to use for parameter population.
	 */
	public AclEntry(AccessControlEntry ace) {
		granting = ace.isGranting();
		id = (String) ace.getId();
		mask = ace.getPermission().getMask();
		order = ace.getAcl().getEntries().indexOf(ace);
		
		if (ace.getSid() instanceof PrincipalSid) {
			sid = ((PrincipalSid) ace.getSid()).getPrincipal();
			sidPrincipal = true;
		} else if (ace.getSid() instanceof GrantedAuthoritySid) {
			sid = ((GrantedAuthoritySid) ace.getSid()).getGrantedAuthority();
			sidPrincipal = false;
		}
		
		if (ace instanceof AuditableAccessControlEntry) {
			auditSuccess = ((AuditableAccessControlEntry) ace).isAuditFailure();
			auditFailure =  ((AuditableAccessControlEntry) ace).isAuditSuccess();
		} else {
			auditSuccess = false;
			auditFailure = false;
		}
	}
	
	/**
	 * @return the identifier of this <code>AclEntry</code>. 
	 * 		The identifier follows the pattern 'objectClass:objectId:sid:order'.
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param id the identifier for this <code>AclEntry</code>. 
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @return true if the Sid for this <code>AclEntry</code> is of type {@link PrincipalSid}
	 * 		of false if it is of type {@link GrantedAuthoritySid}. 
	 */
	public boolean isSidPrincipal() {
		return sidPrincipal;
	}

	/**
	 * @param sidPrincipal whether the Sid for this <code>AclEntry</code> is of type {@link PrincipalSid}.
	 */
	public void setSidPrincipal(boolean sidPrincipal) {
		this.sidPrincipal = sidPrincipal;
	}

	/**
	 * @return the identifier of the Sid for this <code>AclEntry</code>.
	 */
	public String getSid() {
		return sid;
	}
	
	/**
	 * @return the {@link Sid} object for this <code>AclEntry</code>.
	 */
	public Sid getSidObject() {
		Sid result = null;
		if (sidPrincipal) {
			result = new PrincipalSid(sid);
		} else {
			result = new GrantedAuthoritySid(sid);
		}
		return result;
	}

	/**
	 * @param sid the identifier of the Sid for this <code>AclEntry</code>.
	 */
	public void setSid(String sid) {
		this.sid = sid;
	}

	/**
	 * @return the order of this <code>AclEntry</code> in the list of Acl entries for the 
	 * 		related domain object.
	 */
	public int getOrder() {
		return order;
	}

	/**
	 * @param order the order of this <code>AclEntry</code> in the list of Acl entries for the 
	 * 		related domain object.
	 */
	public void setOrder(int order) {
		this.order = order;
	}

	/**
	 * @return the bits that represent the permission.
	 */
	public int getMask() {
		return mask;
	}

	/**
	 * @param mask the bits that represent the permission.
	 */
	public void setMask(int mask) {
		this.mask = mask;
	}

	/**
	 * @return true if permission is being granted, false if is being revoked/blocked.
	 */
	public boolean isGranting() {
		return granting;
	}

	/**
	 * @param granting true if permission is being granted, false if is being revoked/blocked.
	 */
	public void setGranting(boolean granting) {
		this.granting = granting;
	}

	/**
	 * @return true if auditing is enabled for success, false otherwise.
	 */
	public boolean isAuditSuccess() {
		return auditSuccess;
	}

	/**
	 * @param auditSuccess true if auditing is enabled for success, false otherwise.
	 */
	public void setAuditSuccess(boolean auditSuccess) {
		this.auditSuccess = auditSuccess;
	}

	/**
	 * @return true if auditing is enabled for failure, false otherwise.
	 */
	public boolean isAuditFailure() {
		return auditFailure;
	}

	/**
	 * @param auditFailure true if auditing is enabled for failure, false otherwise.
	 */
	public void setAuditFailure(boolean auditFailure) {
		this.auditFailure = auditFailure;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("AclEntry [").append("id: ").append(id);
		sb.append(", sid: ").append(sid);
		sb.append(", sidPrincipal: ").append(sidPrincipal);
		sb.append(", order: ").append(order);
		sb.append(", mask: ").append(mask);
		sb.append(", granting: ").append(granting);
		sb.append(", auditSuccess: ").append(auditSuccess);
		sb.append(", auditFailure: ").append(auditFailure).append(']');
		return sb.toString();
	}

}
