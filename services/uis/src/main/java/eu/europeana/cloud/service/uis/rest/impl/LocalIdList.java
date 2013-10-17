package eu.europeana.cloud.service.uis.rest.impl;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import eu.europeana.cloud.common.model.LocalId;

/**
 * List wrapper for JSON and XML serialization of Provider Ids
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
@XmlRootElement
public class LocalIdList {
    private List<LocalId> list;

    public List<LocalId> getList() {
        return list;
    }

    public void setList(List<LocalId> list) {
        this.list = list;
    }
}
