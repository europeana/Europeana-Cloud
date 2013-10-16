package eu.europeana.cloud.definitions.model;

import javax.xml.bind.annotation.XmlRootElement;
/**
 * Unique Identifier model
 * @author Yorgos.Mamakis@ kb.nl
 *
 */
@XmlRootElement
public class GlobalId {

	/**
	 * The unique identifier
	 */
	String id;
	
	/**
	 * A providerId/recordId combo
	 */
	Provider provider;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Provider getProvider() {
		return provider;
	}

	public void setProvider(Provider provider) {
		this.provider = provider;
	}
	
	
}
