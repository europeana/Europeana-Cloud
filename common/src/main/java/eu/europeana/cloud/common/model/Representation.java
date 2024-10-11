package eu.europeana.cloud.common.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.EqualsAndHashCode;

import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Representation of a record in specific version.
 */
@XmlRootElement
@JacksonXmlRootElement
@JsonRootName(Representation.XSI_TYPE)
@JsonInclude(JsonInclude.Include.NON_NULL)
@EqualsAndHashCode
public class Representation {

  static final String XSI_TYPE = "representation";

  @JacksonXmlProperty(namespace = "http://www.w3.org/2001/XMLSchema-instance", localName = "type", isAttribute = true)
  private final String xsiType = XSI_TYPE;

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

  /*
   * Identifier of the dataset this representation belongs to.
   */
  private String datasetId;

  /**
   * Uri to the history of all versions of this representation.
   */
  private URI allVersionsUri;

  public String getDatasetId() {
    return datasetId;
  }

  public void setDatasetId(String datasetId) {
    this.datasetId = datasetId;
  }

  /**
   * Self uri.
   */
  private URI uri;

  /**
   * Data provider of this version of representation.
   */
  private String dataProvider;

  /**
   * A list of files which constitute this representation.
   */
  @JacksonXmlElementWrapper(useWrapping = false)
  private List<File> files = new ArrayList<>(0);

  /**
   * If this is temporary representation version: date of this object creation; If this is persistent representation version: date
   * of making this object persistent.
   */
  //@XmlJavaTypeAdapter(DateAdapter.class)
  private Date creationDate;

  /**
   * Indicator whether this is persistent representation version (true) or temporary (false).
   */
  private boolean persistent;

  /**
   * A list of revisions which constitute this representation.
   */
  @JacksonXmlElementWrapper(useWrapping = false)
  private List<Revision> revisions = new ArrayList<>(0);

  /**
   * Creates a new instance of this class.
   */
  public Representation() {
    super();
  }


  /**
   * Creates a new instance of this class.
   *
   * @param cloudId cloud identifier
   * @param representationName representation name
   * @param version representation version
   * @param allVersionsUri  uri to all versions
   * @param uri uri to representation
   * @param dataProvider data provider
   * @param files list of files assigned to the representation
   * @param revisions list of revisions assigned to the representation
   * @param persistent  boolean value indicating if representation is persistent
   * @param creationDate representation creation date
   * @param datasetId dataset identifier
   */
  public Representation(String cloudId, String representationName, String version, URI allVersionsUri, URI uri,
      String dataProvider, List<File> files, List<Revision> revisions, boolean persistent, Date creationDate, String datasetId) {
    super();
    this.cloudId = cloudId;
    this.representationName = representationName;
    this.version = version;
    this.allVersionsUri = allVersionsUri;
    this.uri = uri;
    this.dataProvider = dataProvider;
    this.files = files;
    this.revisions = revisions;
    this.persistent = persistent;
    this.creationDate = creationDate;
    this.datasetId = datasetId;
  }


  public Representation(String cloudId, String representationName, String version, URI allVersionsUri, URI uri,
                        String dataProvider, List<File> files, List<Revision> revisions, boolean persistent, Date creationDate) {
    this(cloudId, representationName, version, allVersionsUri, uri, dataProvider, files, revisions, persistent, creationDate, null);
  }

  /**
   * Creates a new instance of this class.
   *
   * @param representation {@link Representation} instance that will be used to construct new one
   */
  public Representation(final Representation representation) {
    this(representation.getCloudId(), representation.getRepresentationName(), representation.getVersion(),
        representation.getAllVersionsUri(), representation.getUri(), representation.getDataProvider(),
        cloneFiles(representation), cloneRevisions(representation), representation.isPersistent(),
        representation.getCreationDate(), representation.getDatasetId());
  }


  public List<Revision> getRevisions() {
    return revisions;
  }

  public void setRevisions(List<Revision> revisions) {
    this.revisions = revisions;
  }


  private static List<File> cloneFiles(Representation representation) {
    List<File> files = new ArrayList<>(representation.getFiles().size());
    for (File file : representation.getFiles()) {
      files.add(new File(file));
    }
    return files;
  }

  private static List<Revision> cloneRevisions(Representation representation) {
    List<Revision> revisions = representation.getRevisions();
    if (revisions == null || revisions.isEmpty()) {
      return new ArrayList<>();
    }
    List<Revision> clonedRevisions = new ArrayList<>(revisions.size());
    for (Revision revision : revisions) {
      clonedRevisions.add(new Revision(revision));
    }
    return clonedRevisions;
  }

  /**
   * Creates new instance of the {@link Representation} class based on provided value
   *
   * @param cloudId cloud identifier
   * @param representationName representation name
   * @param version representation version
   * @return new intance of the {@link Representation} class
   */
  public static Representation fromFields(String cloudId, String representationName, String version) {
    Representation r = new Representation();
    r.setCloudId(cloudId);
    r.setRepresentationName(representationName);
    r.setVersion(version);
    return r;
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


  public String getDataProvider() {
    return dataProvider;
  }


  public void setDataProvider(String dataProvider) {
    this.dataProvider = dataProvider;
  }


  public List<File> getFiles() {
    return files;
  }


  public void setFiles(List<File> files) {
    this.files = files;
  }


  public boolean isPersistent() {
    return persistent;
  }


  public void setPersistent(boolean persistent) {
    this.persistent = persistent;
  }


  public URI getAllVersionsUri() {
    return allVersionsUri;
  }


  public void setAllVersionsUri(URI allVersionsUri) {
    this.allVersionsUri = allVersionsUri;
  }


  public URI getUri() {
    return uri;
  }


  public void setUri(URI selfUri) {
    this.uri = selfUri;
  }


  public Date getCreationDate() {
    return creationDate;
  }


  public void setCreationDate(Date creationDate) {
    this.creationDate = creationDate;
  }

  /**
   * This method is required for @PostFilter (Spring ACL) at RepresentationsResource.getRepresentations()
   */
  @JsonIgnore
  public String getId() {
    return getACLId();
  }

  private String getACLId() {
    return this.getCloudId() + "/" + this.getRepresentationName() + "/" + this.getVersion();
  }

  @Override
  public String toString() {
    return "Representation{" + "cloudId=" + cloudId + ", representationName=" + representationName + ", version="
        + version + ", dataProvider=" + dataProvider + ", files=" + files + ", revisions=" + revisions + ", creationDate="
        + creationDate
        + ", persistent=" + persistent + '}';
  }
}
