package eu.europeana.cloud.common.model;

import javax.xml.bind.annotation.XmlRootElement;
import java.net.URI;
import java.util.Objects;

/**
 * Data set.
 *
 */
@XmlRootElement
public class DataSet {

    /**
     * Data set identifier.
     */
    private String id;

    /**
     * Provider identifier (owner of this data set).     */
    private String providerId;

    /**
     * Description of data set.
     */
    private String description;

    /**
     * Resource URI.
     */
    private URI uri;


    public String getId() {
        return id;
    }


    public void setId(String id) {
        this.id = id;
    }


    public String getProviderId() {
        return providerId;
    }


    public void setProviderId(String providerId) {
        this.providerId = providerId;
    }


    public String getDescription() {
        return description;
    }


    public void setDescription(String description) {
        this.description = description;
    }


    public URI getUri() {
        return uri;
    }


    public void setUri(URI uri) {
        this.uri = uri;
    }


	@Override
	public int hashCode() {
		int hash = 7;
		hash = 97 * hash + Objects.hashCode(this.id);
		hash = 97 * hash + Objects.hashCode(this.providerId);
		hash = 97 * hash + Objects.hashCode(this.description);
		return hash;
	}


	@Override
	public boolean equals(Object obj) {
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final DataSet other = (DataSet) obj;
		if (!Objects.equals(this.id, other.id)) {
			return false;
		}
		if (!Objects.equals(this.providerId, other.providerId)) {
			return false;
		}
		if (!Objects.equals(this.description, other.description)) {
			return false;
		}
		return true;
	}


}
