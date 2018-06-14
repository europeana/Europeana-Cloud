package eu.europeana.cloud.common.model;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Set;

/**
 * Stores set o representation names
 */
@XmlRootElement
public class RepresentationNames {

    private Set<String> names;

    public RepresentationNames() {
    }

    public RepresentationNames(Set<String> names) {
        this.names = names;
    }

    public Set<String> getNames() {
        return names;
    }

    public void setNames(Set<String> names) {
        this.names = names;
    }
}
