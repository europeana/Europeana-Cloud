package eu.europeana.cloud.common.response;

import eu.europeana.cloud.common.model.File;
import jakarta.xml.bind.annotation.XmlRootElement;

import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Representation of a record in specific version.
 */
@Data
@AllArgsConstructor
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
   * Uri of the representation version
   */
  private URI representationVersionUri;


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
    this(response.getCloudId(), response.getRepresentationName(), response.getVersion(), response.getRepresentationVersionUri(),
        cloneFiles(response), response.getRevisionProviderId(), response.getRevisionName(), response.getRevisionTimestamp());
  }


  private static List<File> cloneFiles(RepresentationRevisionResponse representation) {
    List<File> files = new ArrayList<>(representation.getFiles().size());
    for (File file : representation.getFiles()) {
      files.add(new File(file));
    }
    return files;
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

}
