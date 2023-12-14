package eu.europeana.cloud.common.response;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.EqualsAndHashCode;

import java.util.Objects;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Association between cloud identifier and tags of its revision
 */
@XmlRootElement
@JsonRootName(CloudTagsResponse.XSI_TYPE)
@EqualsAndHashCode
public class CloudTagsResponse implements Comparable {

  static final String XSI_TYPE = "cloudTagsResponse";

  @JacksonXmlProperty(namespace = "http://www.w3.org/2001/XMLSchema-instance", localName = "type", isAttribute = true)
  private final String xsiType = XSI_TYPE;

  /**
   * Identifier (cloud id) of a record.
   */
  private String cloudId;

  /**
   * Deleted tag
   */
  private boolean deleted;

  /**
   * Creates a new instance of this class.
   */
  public CloudTagsResponse() {
    super();
  }


  /**
   * Creates a new instance of this class.
   *
   * @param cloudId
   * @param deleted
   */
  public CloudTagsResponse(String cloudId, boolean deleted) {
    super();
    this.cloudId = cloudId;
    this.deleted = deleted;
  }


  public String getCloudId() {
    return cloudId;
  }

  public void setCloudId(String cloudId) {
    this.cloudId = cloudId;
  }

  public boolean isDeleted() {
    return deleted;
  }


  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }

  @Override
  public String toString() {
    return "CloudTags{" + "cloudId=" + cloudId + ", deleted=" + deleted + "}";
  }

  @Override
  public int compareTo(Object o) {
    if (o == null) {
      return 1;
    }

    CloudTagsResponse other = (CloudTagsResponse) o;
    if (this.cloudId.equals(other.cloudId)) {
      return Boolean.compare(this.deleted, other.deleted);
    }
    return this.cloudId.compareTo(other.cloudId);
  }
}
