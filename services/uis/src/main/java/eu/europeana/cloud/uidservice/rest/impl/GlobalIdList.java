package eu.europeana.cloud.uidservice.rest.impl;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import eu.europeana.cloud.definitions.model.GlobalId;

/**
 * List wrapper for JSON and XML serialization of Unique Identifiers
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
@XmlRootElement
public class GlobalIdList {

	List<GlobalId> list;

	public List<GlobalId> getList() {
		return list;
	}

	public void setList(List<GlobalId> list) {
		this.list = list;
	}
}
