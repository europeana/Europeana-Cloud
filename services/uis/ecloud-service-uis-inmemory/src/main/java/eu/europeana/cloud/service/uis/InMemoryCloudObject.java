package eu.europeana.cloud.service.uis;

/**
 * In Memory mockup of a Database Object
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
public class InMemoryCloudObject {

	/**
	 * Cloud Identifier
	 */
	private String cloudId;
	
	/**
	 * Provider Identifier
	 */
	private String providerId;
	
	/**
	 * Record Identifier
	 */
	private String recordId;
	
	/**
	 * Deleted flag
	 */
	private boolean deleted;

	public String getCloudId() {
		return cloudId;
	}

	public void setCloudId(String cloudId) {
		this.cloudId = cloudId;
	}

	public String getProviderId() {
		return providerId;
	}

	public void setProviderId(String providerId) {
		this.providerId = providerId;
	}

	public String getRecordId() {
		return recordId;
	}

	public void setRecordId(String recordId) {
		this.recordId = recordId;
	}

	public boolean isDeleted() {
		return deleted;
	}

	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}

}
