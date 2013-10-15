package eu.europeana.cloud.uidservice.rest.impl;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import eu.europeana.cloud.definitions.model.Provider;

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
