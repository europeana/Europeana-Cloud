package eu.europeana.cloud.service.uis.rest.impl;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

import eu.europeana.cloud.common.model.GlobalId;

/**
 * List wrapper for JSON and XML serialization of Unique Identifiers
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 17, 2013
 */
@XmlRootElement
@XmlSeeAlso({GlobalId.class})
public class GlobalIdList {
    private List<GlobalId> list;

    public List<GlobalId> getList() {
        return list;
    }

    public void setList(List<GlobalId> list) {
        this.list = list;
    }
}
