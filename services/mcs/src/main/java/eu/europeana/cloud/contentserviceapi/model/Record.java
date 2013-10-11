package eu.europeana.cloud.contentserviceapi.model;

import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

/**
 * Record
 */
@XmlRootElement
public class Record {

    private String id;

    private List<Representation> representations;


    public String getId() {
        return id;
    }


    public void setId(String id) {
        this.id = id;
    }


    public List<Representation> getRepresentations() {
        return representations;
    }


    public void setRepresentations(List<Representation> representations) {
        this.representations = representations;
    }
}
