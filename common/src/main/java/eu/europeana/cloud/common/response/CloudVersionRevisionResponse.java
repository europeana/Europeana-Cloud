package eu.europeana.cloud.common.response;

import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.EqualsAndHashCode;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Association between cloud identifier, version identifier and revision identifier.
 */
@XmlRootElement
@JsonRootName(CloudVersionRevisionResponse.XSI_TYPE)
@EqualsAndHashCode
public class CloudVersionRevisionResponse implements Comparable {

  static final String XSI_TYPE = "cloudVersionRevisionResponse";

  @JacksonXmlProperty(namespace = "http://www.w3.org/2001/XMLSchema-instance", localName = "type", isAttribute = true)
  private final String xsiType = XSI_TYPE;

  /**
   * Identifier (cloud id) of a record.
   */
  private String cloudId;

  /**
   * Identifier of a version.
   */
  private String version;

  /**
   * Identifier of a revision.
   */
  private String revisionId;

  /**
   * Deleted tag
   */
  private boolean deleted;

  /**
   * Creates a new instance of this class.
   */
  public CloudVersionRevisionResponse() {
    super();
  }


  /**
   * Creates a new instance of this class.
   *
   * @param cloudId
   * @param version
   * @param revisionId
   */
  public CloudVersionRevisionResponse(String cloudId, String version, String revisionId, boolean deleted) {
    super();
    this.cloudId = cloudId;
    this.version = version;
    this.revisionId = revisionId;
    this.deleted = deleted;
  }


  public String getCloudId() {
    return cloudId;
  }


  public void setCloudId(String cloudId) {
    this.cloudId = cloudId;
  }


  public String getRevisionId() {
    return revisionId;
  }


  public void setRevisionId(String revisionId) {
    this.revisionId = revisionId;
  }


  public String getVersion() {
    return version;
  }


  public void setVersion(String version) {
    this.version = version;
  }

  @Override
  public String toString() {
    return "CloudVersionRevision{" + "cloudId=" + cloudId + ", version="
            + version + ", revisionId=" + revisionId
            + ", deleted=" + deleted + "}";
  }

  @Override
  public int compareTo(Object o) {
    if (o == null) {
      return 1;
    }

    CloudVersionRevisionResponse other = (CloudVersionRevisionResponse) o;
    if (this.cloudId.equals(other.cloudId)) {
      if (this.version.equals(other.version)) {
        if (this.revisionId.equals(other.revisionId)) {
          return Boolean.valueOf(this.deleted).compareTo(Boolean.valueOf(other.deleted));
        }
        return this.revisionId.compareTo(other.revisionId);
      }
      return this.version.compareTo(other.version);
    }
    return this.cloudId.compareTo(other.cloudId);
  }

  public boolean isDeleted() {
    return deleted;
  }

  public void setDeleted(boolean deleted) {
    this.deleted = deleted;
  }
}
