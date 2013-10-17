package eu.europeana.cloud.service.uis.rest.impl;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import eu.europeana.cloud.common.model.Provider;

/**
 * List wrapper for JSON and XML serialization of Provider Ids
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
@XmlRootElement
public class ProviderList {

	List<Provider> list;

	public List<Provider> getList() {
		return list;
	}

	public void setList(List<Provider> list) {
		this.list = list;
	}
}
