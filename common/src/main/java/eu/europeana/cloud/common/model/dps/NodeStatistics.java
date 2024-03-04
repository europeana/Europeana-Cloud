package eu.europeana.cloud.common.model.dps;

import jakarta.xml.bind.annotation.XmlRootElement;

import java.util.HashSet;
import java.util.Set;

/**
 * Statistics for a node.
 */
@XmlRootElement
public class NodeStatistics {

  /**
   * Parent xpath
   */
  private String parentXpath;

  /**
   * Node xpath
   */
  private String xpath;

  /**
   * Node value
   */
  private String value;

  /**
   * Node occurrence
   */
  private long occurrence;

  /**
   * List of attributes together with their statistics
   */
  private Set<AttributeStatistics> attributesStatistics = new HashSet<>();

  public NodeStatistics() {
  }


  public NodeStatistics(String parentXpath, String xpath, String value, long occurrence) {
    this(parentXpath, xpath, value, occurrence, new HashSet<>());
  }

  public NodeStatistics(String parentXpath, String xpath, String value, long occurrence,
      Set<AttributeStatistics> attributesStatistics) {
    this.parentXpath = parentXpath == null ? "" : parentXpath;
    this.xpath = xpath;
    this.value = value;
    this.occurrence = occurrence <= 0 ? 1 : occurrence;
    this.attributesStatistics = attributesStatistics;
  }

  public String getParentXpath() {
    return parentXpath;
  }

  public String getXpath() {
    return xpath;
  }

  public String getValue() {
    return value;
  }

  public long getOccurrence() {
    return occurrence;
  }

  public void increaseOccurrence() {
    this.occurrence++;
  }

  public Set<AttributeStatistics> getAttributesStatistics() {
    return attributesStatistics;
  }

  public void setAttributesStatistics(Set<AttributeStatistics> attributesStatistics) {
    this.attributesStatistics = attributesStatistics;
  }

  public void setParentXpath(String parentXpath) {
    this.parentXpath = parentXpath;
  }

  public void setXpath(String xpath) {
    this.xpath = xpath;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public void setOccurrence(long occurrence) {
    this.occurrence = occurrence;
  }

  public boolean hasAttributes() {
    return !attributesStatistics.isEmpty();
  }

  @Override
  public boolean equals(Object o) {

    if (o == this) {
      return true;
    }
    if (o == null || this.getClass() != o.getClass()) {
      return false;
    }

    NodeStatistics nodeStatistics = (NodeStatistics) o;

    return nodeStatistics.getParentXpath().equals(parentXpath) &&
        nodeStatistics.getValue().equals(value) &&
        nodeStatistics.getXpath().equals(xpath) &&
        nodeStatistics.getAttributesStatistics().equals(attributesStatistics);
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 31 * result + parentXpath.hashCode();
    result = 31 * result + xpath.hashCode();
    result = 31 * result + value.hashCode();
    result = 31 * result + attributesStatistics.hashCode();
    return result;
  }
}
