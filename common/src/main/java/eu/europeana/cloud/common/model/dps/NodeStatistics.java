package eu.europeana.cloud.common.model.dps;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Statistics for a node.
 */
public class NodeStatistics {
    /** Parent xpath */
    private final String parentXpath;

    /** Node xpath */
    private final String xpath;

    /** Node value */
    private final String value;

    /** Node occurrence */
    private long occurrence;

    /** List of attributes together with their statistics */
    private Set<AttributeStatistics> attributesStatistics = new HashSet<>();

    public NodeStatistics(String parentXpath, String xpath, String value, long occurrence) {
        this(parentXpath, xpath, value, occurrence, new HashSet<AttributeStatistics>());
    }

    public NodeStatistics(String parentXpath, String xpath, String value, long occurrence, Set<AttributeStatistics> attributesStatistics) {
        this.parentXpath = parentXpath;
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

    public Set<AttributeStatistics> getAttributesStatistics() {
        return attributesStatistics;
    }

    public void setAttributesStatistics(Set<AttributeStatistics> attributesStatistics) {
        this.attributesStatistics = attributesStatistics;
    }

    public boolean hasAttributes() {
        return !attributesStatistics.isEmpty();
    }

    @Override
    public boolean equals(Object o) {

        if (o == this) return true;
        if (!(o instanceof NodeStatistics)) {
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
