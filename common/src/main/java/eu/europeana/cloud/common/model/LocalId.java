package eu.europeana.cloud.common.model;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * The provider Id/local Id model
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
@XmlRootElement
public class LocalId {
	/**
	 * provider id
	 */
	private String providerId;

	/**
	 * record id
	 */
	private String recordId;

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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((providerId == null) ? 0 : providerId.hashCode());
		result = prime * result + ((recordId == null) ? 0 : recordId.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object e) {
		if (e==null || !e.getClass().isAssignableFrom(LocalId.class)) {
			return false;
		}
		if (!(this.providerId.contentEquals(((LocalId) e).getProviderId())
				&& this.recordId.contentEquals(((LocalId) e).getRecordId()))) {
			return false;
		}
		return true;
	}

	@Override
	public String toString(){
		return String.format("{\nproviderId: %s\n recordId: %s\n}", this.providerId, this.recordId);
	}
}
