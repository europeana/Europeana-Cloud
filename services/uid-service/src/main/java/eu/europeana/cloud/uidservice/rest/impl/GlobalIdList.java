package eu.europeana.cloud.uidservice.rest.impl;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import eu.europeana.cloud.definitions.model.GlobalId;
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
