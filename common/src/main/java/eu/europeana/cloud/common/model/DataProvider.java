package eu.europeana.cloud.common.model;

public class DataProvider {

    /**
     * The provider id
     */
    String id;
    /**
     * Data provider properties
     */
    DataProviderProperties properties;

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
}
