package eu.europeana.cloud.common.model;

import java.net.URI;

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
}
