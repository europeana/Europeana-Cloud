package eu.europeana.cloud.common.model;

import javax.xml.bind.annotation.XmlRootElement;
/**
 * The provider Id/record Id model
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
@XmlRootElement
public class Provider {

	/**
	 * The provider id
	 */
	String id;
	
	/**
	 * The record id
	 */
	String recordId;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getRecordId() {
		return recordId;
	}

	public void setRecordId(String recordId) {
		this.recordId = recordId;
	}
}
