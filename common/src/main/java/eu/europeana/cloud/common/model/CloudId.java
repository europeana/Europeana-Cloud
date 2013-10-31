package eu.europeana.cloud.common.model;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Unique Identifier model
 * 
 * @author Yorgos.Mamakis@ kb.nl
 * 
 */
@XmlRootElement
public class CloudId {
    /**
     * The unique identifier
     */
    private String  id;

    /**
     * A providerId/recordId combo
     */
    private LocalId localId;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public LocalId getLocalId() {
        return localId;
    }

    public void setLocalId(LocalId localId) {
        this.localId = localId;
    }
}
