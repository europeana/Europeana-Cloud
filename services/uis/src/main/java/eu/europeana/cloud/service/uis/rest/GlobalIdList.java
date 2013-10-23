package eu.europeana.cloud.service.uis.rest;

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
@XmlSeeAlso({ GlobalId.class })
public class GlobalIdList {
    /**
     * list of global Ids
     */
    private List<GlobalId> list;

    /**
     * @return list of global Ids
     */
    public List<GlobalId> getList() {
        return list;
    }

    /**
     * @param list
     *            list of global Ids
     */
    public void setList(List<GlobalId> list) {
        this.list = list;
    }
}
