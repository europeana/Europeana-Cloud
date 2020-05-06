package eu.europeana.cloud.service.mcs.utils;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import eu.europeana.cloud.common.model.Representation;
import lombok.Getter;

import java.util.List;

@Getter
@JsonRootName("representations")
public class RepresentationsListWrapper {
    @JacksonXmlProperty(localName = "representation")
    @JacksonXmlElementWrapper(useWrapping = false)
    private List<Representation> representations;

    public RepresentationsListWrapper(List<Representation> representations) {
        this.representations = representations;
    }
}
