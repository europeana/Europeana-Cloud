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
	public boolean equals(Object e) {
		if (!e.getClass().isAssignableFrom(LocalId.class)) {
			return false;
		}
		if (!(this.providerId.contentEquals(((LocalId) e).getProviderId())
				&& this.recordId.contentEquals(((LocalId) e).getRecordId()))) {
			return false;
		}
		return true;
	}
}
