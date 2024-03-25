package eu.europeana.cloud.common.model;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Unique Identifier model
 */
@XmlRootElement
@JsonRootName(CloudId.XSI_TYPE)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CloudId {

  static final String XSI_TYPE = "cloudId";

  @JacksonXmlProperty(namespace = "http://www.w3.org/2001/XMLSchema-instance", localName = "type", isAttribute = true)
  private final String xsiType = XSI_TYPE;

  /* The unique identifier */
  private String id;

  /* A providerId/recordId combo */
  private LocalId localId;

  @Override
  public String toString() {
    return String.format("{%ncloudId: %s%n record: %s%n}", this.id, this.localId.toString());
  }
}
