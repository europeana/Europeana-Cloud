package eu.europeana.cloud.service.mcs.utils;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@JsonRootName("representationRevisionResponses")
@NoArgsConstructor
public class RepresentationRevisionResponseListWrapper {

  @JacksonXmlProperty(localName = "representationRevisionResponse")
  @JacksonXmlElementWrapper(useWrapping = false)
  private List<RepresentationRevisionResponse> representationRevisions;

  public RepresentationRevisionResponseListWrapper(List<RepresentationRevisionResponse> representations) {
    this.representationRevisions = representations;
  }
}
