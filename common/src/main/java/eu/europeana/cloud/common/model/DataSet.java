package eu.europeana.cloud.common.model;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.EqualsAndHashCode;

import java.net.URI;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Data set.
 */
@XmlRootElement
@JsonRootName(DataSet.XSI_TYPE)
@EqualsAndHashCode
public class DataSet {

  static final String XSI_TYPE = "dataSet";

  @JacksonXmlProperty(namespace = "http://www.w3.org/2001/XMLSchema-instance", localName = "type", isAttribute = true)
  private final String xsiType = XSI_TYPE;

  /**
   * Data set identifier.
   */
  private String id;

  /**
   * Provider identifier (owner of this data set).
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
