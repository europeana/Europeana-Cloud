package eu.europeana.cloud.common.model;

import java.net.URI;
import java.util.Objects;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class DataProvider {

    /**
     * The provider id.
     */
    String id;

    /**
     * Data provider properties.
     */
    DataProviderProperties properties;

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


    public DataProviderProperties getProperties() {
        return properties;
    }


    public void setProperties(DataProviderProperties properties) {
        this.properties = properties;
    }


    public URI getUri() {
        return uri;
    }


    public void setUri(URI uri) {
        this.uri = uri;
    }


    @Override
    public int hashCode() {
        int hash = 5;
        hash = 37 * hash + Objects.hashCode(this.id);
        hash = 37 * hash + Objects.hashCode(this.properties);
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
        final DataProvider other = (DataProvider) obj;
        if (!Objects.equals(this.id, other.id)) {
            return false;
        }
        if (!Objects.equals(this.properties, other.properties)) {
            return false;
        }
        return true;
    }
}
