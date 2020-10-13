package eu.europeana.cloud.common.model;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Unique Identifier model
 *
 * @author Yorgos.Mamakis@ kb.nl
 */
@XmlRootElement
@JsonRootName(CloudId.XSI_TYPE)
public class CloudId {

    final static String XSI_TYPE = "cloudId";

    @JacksonXmlProperty(namespace = "http://www.w3.org/2001/XMLSchema-instance", localName = "type", isAttribute = true)
    private final String xsiType = XSI_TYPE;

    /* The unique identifier */
    private String id;

    /* A providerId/recordId combo */
    private LocalId localId;

    /**
     * @return The cloud identifier
     */
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return A providerId/recordId combo
     */
    public LocalId getLocalId() {
        return localId;
    }

    public void setLocalId(LocalId localId) {
        this.localId = localId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((localId == null) ? 0 : localId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object e) {
        if (e == null || !e.getClass().isAssignableFrom(CloudId.class)) {
            return false;
        }
        if (!(this.id.contentEquals(((CloudId) e).getId()) && this.localId.equals(((CloudId) e).getLocalId()))) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return String.format("{%ncloudId: %s%n record: %s%n}", this.id, this.localId.toString());
    }
}
