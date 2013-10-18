
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
    
    public URI getUri() {
        return uri;
    }

    public void setUri(URI uri) {
        this.uri = uri;
    }
   
}
