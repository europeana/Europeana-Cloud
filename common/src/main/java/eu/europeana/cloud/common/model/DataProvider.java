package eu.europeana.cloud.common.model;

import java.net.URI;
import java.util.Objects;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Represents data provider.
 * 
 */
@XmlRootElement
public class DataProvider {

	public DataProvider() {
	}
	
	public DataProvider(final String id) {
		this.id = id;
	}
	
    /**
     * The provider id.
     */
    private String id;

    /**
     * The hash of provider id.
     */
    private int partitionKey;

    /**
     * Data provider properties.
     */
    private DataProviderProperties properties;

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


    public void setPartitionKey(int partitionKey) {
        this.partitionKey = partitionKey;
    }


    public int getPartitionKey() {
        return partitionKey;
    }


    @Override
    public int hashCode() {
        int hash = 5;
        hash = 37 * hash + Objects.hashCode(this.id);
        hash = 37 * hash + Objects.hashCode(this.partitionKey);
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
        if (!Objects.equals(this.partitionKey, other.partitionKey)) {
            return false;
        }
        if (!Objects.equals(this.properties, other.properties)) {
            return false;
        }
        return true;
    }
    
    @Override
    public String toString() {
    	return super.toString();
    }
}
