package eu.europeana.cloud.service.uis.dao;

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

	/**
	 * @return cloudId
	 */
	public String getCloudId() {
		return cloudId;
	}

	/**
	 * 
	 * @param cloudId
	 */
	public void setCloudId(String cloudId) {
		this.cloudId = cloudId;
	}

	/**
	 * 
	 * @return providerId
	 */
	public String getProviderId() {
		return providerId;
	}

	/**
	 * 
	 * @param providerId
	 */
	public void setProviderId(String providerId) {
		this.providerId = providerId;
	}

	/**
	 * 
	 * @return recordId
	 */
	public String getRecordId() {
		return recordId;
	}

	/**
	 * 
	 * @param recordId
	 */
	public void setRecordId(String recordId) {
		this.recordId = recordId;
	}

	/**
	 * 
	 * @return deleted
	 */
	public boolean isDeleted() {
		return deleted;
	}

	/**
	 * 
	 * @param deleted
	 */
	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}

}
