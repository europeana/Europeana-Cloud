package eu.europeana.cloud.common.model;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.EqualsAndHashCode;

import java.net.URI;

/**
 * Represents data provider.
 */
@XmlRootElement
@JsonRootName(DataProvider.XSI_TYPE)
@EqualsAndHashCode
public class DataProvider {

  static final String XSI_TYPE = "dataProvider";

  @JacksonXmlProperty(namespace = "http://www.w3.org/2001/XMLSchema-instance", localName = "type", isAttribute = true)
  private final String xsiType = XSI_TYPE;

  /**
   * The provider id.
   */
  private String id;

  /**
   * The hash of provider id.
   */
  private int partitionKey;

  /* Indicates if data-provider is active or not */
  private boolean active = true;

  /**
   * Data provider properties.
   */
  private DataProviderProperties properties;

  /**
   * Resource URI.
   */
  private URI uri;

  public DataProvider() {
  }

  /**
   * Creates instance of {@link DataProvider} class with the specified identifier
   *
   * @param id identifier that will be used for created data-provider
   */
  public DataProvider(final String id) {
    this.id = id;
  }


  public String getId() {
    return id;
  }


  public void setId(String id) {
    this.id = id;
  }


  public DataProviderProperties getProperties() {
    return properties;
  }


  public void setProperties(DataProviderProperties properties) {
    this.properties = properties;
  }


  public URI getUri() {
    return uri;
  }


  public void setUri(URI uri) {
    this.uri = uri;
  }


  public void setPartitionKey(int partitionKey) {
    this.partitionKey = partitionKey;
  }


  public int getPartitionKey() {
    return partitionKey;
  }

  public void setActive(boolean active) {
    this.active = active;
  }

  /**
   * @return Indicates if data-provider is active or not
   */
  public boolean isActive() {
    return active;
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
