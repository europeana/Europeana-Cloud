package eu.europeana.cloud.service.uis.rest;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

import eu.europeana.cloud.common.model.LocalId;

/**
 * List wrapper for JSON and XML serialization of Provider Ids
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 17, 2013
 */
@XmlRootElement
@XmlSeeAlso({ LocalId.class })
public class LocalIdList {
    /**
     * list of local Ids
     */
    private List<LocalId> list;

    /**
     * @return list of local Ids
     */
    public List<LocalId> getList() {
        return list;
    }

    /**
     * @param list
     *            list of local Ids
     */
    public void setList(List<LocalId> list) {
        this.list = list;
    }
}
