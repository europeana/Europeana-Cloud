package eu.europeana.cloud.common.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonRootName;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * Record with its representations.
 */
@XmlRootElement
@JsonRootName("record")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Record {

  /**
   * Identifier (cloud id) of a record.
   */
  private String cloudId;

  /**
   * List of representations of this record.
   */
  @JacksonXmlElementWrapper(useWrapping = false)
  private List<Representation> representations;


  /**
   * Constructs new instance of {@link Record} class with empty list of representations.
   */
  public Record() {
    super();

    this.cloudId = null;
    this.representations = new ArrayList<>();
  }


  /**
   * Constructs new instance of {@link Record} class with specified cloudId and list of representations
   *
   * @param cloudId cloud identifier that will identify created {@link Record}
   * @param representations list of representations that will be assigned to the created {@link Record}
   */
  public Record(String cloudId, List<Representation> representations) {
    super();
    this.cloudId = cloudId;
    this.representations = representations;
  }


  /**
   * Constructs new instance of {@link Record} class based on provided one
   *
   * @param record {@link Record} instance that will be used to construct new one
   */
  public Record(final Record record) {
    this(record.getCloudId(), cloneRepresentations(record.getRepresentations()));
  }


  private static List<Representation> cloneRepresentations(List<Representation> representations) {
    List<Representation> newRepresentations = new ArrayList<>(representations.size());
    for (Representation representation : representations) {
      newRepresentations.add(new Representation(representation));
    }
    return newRepresentations;
  }


  public String getCloudId() {
    return cloudId;
  }


  public void setCloudId(String cloudId) {
    this.cloudId = cloudId;
  }


  public List<Representation> getRepresentations() {
    return representations;
  }


  public void setRepresentations(List<Representation> representations) {
    this.representations = representations;
  }


  @Override
  public String toString() {
    return "Record [cloudId=" + cloudId + ", representations=" + representations + "]";
  }


  @Override
  public int hashCode() {

    final int prime = 31;
    int result = 1;
    result = prime * result + ((cloudId == null) ? 0 : cloudId.hashCode());
    result = prime * result + ((representations == null) ? 0 : representations.hashCode());
    return result;
  }


  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    Record other = (Record) obj;
    if (cloudId == null) {
      if (other.cloudId != null) {
        return false;
      }
    } else if (!cloudId.equals(other.cloudId)) {
      return false;
    }
    if (representations == null) {
      if (other.representations != null) {
        return false;
      }
    } else if (!representations.equals(other.representations)) {
      return false;
    }
    return true;
  }

}
