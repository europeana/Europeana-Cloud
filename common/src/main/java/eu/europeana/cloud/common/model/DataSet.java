package eu.europeana.cloud.common.model;

import java.net.URI;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class DataSet {

    /**
     * Data set identifier.
     */
    private String id;

    /**
     * Provider identifier.
     */
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
}
