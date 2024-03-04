package eu.europeana.cloud.common.model;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The provider Id/local Id model
 */
@XmlRootElement
@JsonRootName(LocalId.XSI_TYPE)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class LocalId {

  static final String XSI_TYPE = "localId";

  @JacksonXmlProperty(namespace = "http://www.w3.org/2001/XMLSchema-instance", localName = "type", isAttribute = true)
  private final String xsiType = XSI_TYPE;

  /* Provider id */
  private String providerId;

  /* Record id */
  private String recordId;

  @Override
  public String toString() {
    return String.format("{%nproviderId: %s%n recordId: %s%n}", this.providerId, this.recordId);
  }
}
