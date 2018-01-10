package eu.europeana.cloud.common.model.dps;

import java.util.ArrayList;
import java.util.List;

/**
 * Statistics for a node.
 */
public class NodeStatistics {
    /** Node xpath */
    private final String xpath;

    /** Node value */
    private final String value;

    /** Node occurrence */
    private int occurrence;

    /** List of attributes together with their statistics */
    private List<AttributeStatistics> attributesStatistics = new ArrayList<>();

    public NodeStatistics(String xpath, String value, int occurrence) {
        this(xpath, value, occurrence, new ArrayList<AttributeStatistics>());
    }

    public NodeStatistics(String xpath, String value, int occurrence, List<AttributeStatistics> attributesStatistics) {
        this.xpath = xpath;
        this.value = value;
        this.occurrence = occurrence;
        this.attributesStatistics = attributesStatistics;
    }

    public String getXpath() {
        return xpath;
    }

    public String getValue() {
        return value;
    }

    public int getOccurrence() {
        return occurrence;
    }

    public List<AttributeStatistics> getAttributesStatistics() {
        return attributesStatistics;
    }

    public void setAttributesStatistics(List<AttributeStatistics> attributesStatistics) {
        this.attributesStatistics = attributesStatistics;
    }

    /**
     * Equals compares only xpath and value fields as they are significant when trying to identify the object.
     *
     * @param obj object to check
     * @return true when obj is the same object or when it has both xpath and value equal to this object's xpath and value
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!obj.getClass().equals(this.getClass())) {
            return false;
        }
        NodeStatistics stats = (NodeStatistics) obj;
        return (this.xpath.equals(stats.xpath) && this.value.equals(stats.value));
    }
}
