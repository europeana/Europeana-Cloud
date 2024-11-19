package eu.europeana.cloud.service.mcs.utils;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import eu.europeana.cloud.common.response.RepresentationRevisionResponse;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Wrapper for the list of representation to achieve proper XML serialisation for the list
 * compatible with our clients based on Jersey library.
 */
@Getter
@JsonRootName("representationRevisionResponses")
@NoArgsConstructor
public class RepresentationRevisionResponseListWrapper {

  @JacksonXmlProperty(localName = "representationRevisionResponse")
  @JacksonXmlElementWrapper(useWrapping = false)
  private List<RepresentationRevisionResponse> representationRevisions;

  /**
   * The constructor
   * @param representationRevisions - list of representations
   */
  public RepresentationRevisionResponseListWrapper(List<RepresentationRevisionResponse> representationRevisions) {
    this.representationRevisions = new ArrayList<>(representationRevisions);
  }
}
