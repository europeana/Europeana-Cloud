package eu.europeana.cloud.definitions.model;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Provider {

	String id;
	
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
