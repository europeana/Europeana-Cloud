package eu.europeana.cloud.service.uis.rest;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSeeAlso;

import eu.europeana.cloud.common.model.CloudId;

/**
 * List wrapper for JSON and XML serialization of Unique Identifiers
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * @since Oct 17, 2013
 */
@XmlRootElement
@XmlSeeAlso({ CloudId.class })
public class CloudIdList {
    /**
     * list of global Ids
     */
    private List<CloudId> list;

    /**
     * @return list of global Ids
     */
    public List<CloudId> getList() {
        return list;
    }

    /**
     * @param list
     *            list of global Ids
     */
    public void setList(List<CloudId> list) {
        this.list = list;
    }
}
