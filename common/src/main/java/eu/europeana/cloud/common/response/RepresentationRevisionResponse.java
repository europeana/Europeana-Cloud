package eu.europeana.cloud.common.response;

import eu.europeana.cloud.common.model.File;
import jakarta.xml.bind.annotation.XmlRootElement;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

/**
 * Representation of a record in specific version.
 */
@XmlRootElement
public class RepresentationRevisionResponse {

  /**
   * Identifier (cloud id) of a record this object is representation of.
   */
  private String cloudId;

  /**
   * Representation Name of this representation.
   */
  private String representationName;

  /**
   * Identifier of a version of this representation.
   */
  private String version;


  /**
   * A list of files which constitute this representation.
   */
  private List<File> files = new ArrayList<>(0);


  /**
   * Revision provider identifier.
   */
  private String revisionProviderId;


  /**
   * Revision name.
   */
  private String revisionName;


  /**
   * Revision timestamp
   */
  private Date revisionTimestamp;

  public RepresentationRevisionResponse() {
  }

  /**
   * Creates a new instance of this class.
   *
   * @param cloudId
   * @param representationName
   * @param version
   * @param revisionProviderId
   * @param revisionName
   * @param revisionTimestamp
   */
  public RepresentationRevisionResponse(String cloudId, String representationName, String version,
      String revisionProviderId, String revisionName, Date revisionTimestamp) {
    this.cloudId = cloudId;
    this.representationName = representationName;
    this.version = version;
    this.revisionProviderId = revisionProviderId;
    this.revisionName = revisionName;
    this.revisionTimestamp = revisionTimestamp;
  }

  /**
   * Creates a new instance of this class.
   *
   * @param cloudId
   * @param representationName
   * @param version
   * @param files
   * @param revisionProviderId
   * @param revisionName
   */
  public RepresentationRevisionResponse(String cloudId, String representationName, String version,
      List<File> files, String revisionProviderId, String revisionName,
      Date revisionTimestamp) {
    this.cloudId = cloudId;
    this.representationName = representationName;
    this.version = version;
    this.files = files;
    this.revisionProviderId = revisionProviderId;
    this.revisionName = revisionName;
    this.revisionTimestamp = revisionTimestamp;
  }


  /**
   * Creates a new instance of this class.
   *
   * @param response
   */
  public RepresentationRevisionResponse(final RepresentationRevisionResponse response) {
    this(response.getCloudId(), response.getRepresentationName(), response.getVersion(),
        cloneFiles(response), response.getRevisionProviderId(), response.getRevisionName(), response.getRevisionTimestamp());
  }


  private static List<File> cloneFiles(RepresentationRevisionResponse representation) {
    List<File> files = new ArrayList<>(representation.getFiles().size());
    for (File file : representation.getFiles()) {
      files.add(new File(file));
    }
    return files;
  }


  public String getCloudId() {
    return cloudId;
  }


  public void setCloudId(String cloudId) {
    this.cloudId = cloudId;
  }


  public String getRepresentationName() {
    return representationName;
  }


  public void setRepresentationName(String representationName) {
    this.representationName = representationName;
  }


  public String getVersion() {
    return version;
  }


  public void setVersion(String version) {
    this.version = version;
  }


  public List<File> getFiles() {
    return files;
  }


  public void setFiles(List<File> files) {
    this.files = files;
  }


  public String getRevisionProviderId() {
    return revisionProviderId;
  }


  public void setRevisionProviderId(String revisionProviderId) {
    this.revisionProviderId = revisionProviderId;
  }


  public String getRevisionName() {
    return revisionName;
  }


  public void setRevisionName(String revisionName) {
    this.revisionName = revisionName;
  }

  public Date getRevisionTimestamp() {
    return revisionTimestamp;
  }

  public void setRevisionTimestamp(Date revisionTimestamp) {
    this.revisionTimestamp = revisionTimestamp;
  }

  /**
   * This method is required for @PostFilter (Spring ACL) at RepresentationsResource.getRepresentations()
   */
  public String getId() {
    return getACLId();
  }

  private String getACLId() {
    return this.getCloudId() + "/" + this.getRepresentationName() + "/" + this.getVersion();
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 37 * hash + Objects.hashCode(this.cloudId);
    hash = 37 * hash + Objects.hashCode(this.representationName);
    hash = 37 * hash + Objects.hashCode(this.version);
    hash = 37 * hash + Objects.hashCode(this.files);
    hash = 37 * hash + Objects.hashCode(this.revisionProviderId);
    hash = 37 * hash + Objects.hashCode(this.revisionName);
    hash = 37 * hash + Objects.hashCode(this.revisionTimestamp);
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final RepresentationRevisionResponse other = (RepresentationRevisionResponse) obj;
    if (!Objects.equals(this.cloudId, other.cloudId)) {
      return false;
    }
    if (!Objects.equals(this.representationName, other.representationName)) {
      return false;
    }
    if (!Objects.equals(this.version, other.version)) {
      return false;
    }
    if (!Objects.equals(this.files, other.files)) {
      return false;
    }
    if (!Objects.equals(this.revisionProviderId, other.revisionProviderId)) {
      return false;
    }
    if (!Objects.equals(this.revisionName, other.revisionName)) {
      return false;
    }
    if (!Objects.equals(this.revisionTimestamp, other.revisionTimestamp)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "RepresentationRevisionResponse{" + "cloudId=" + cloudId
        + ", representationName=" + representationName + ", version=" + version
        + ", files=" + files + ", revisionProviderId=" + revisionProviderId
        + ", revisionName=" + revisionName + ", revisionTimestamp=" + revisionTimestamp + '}';
  }
}
