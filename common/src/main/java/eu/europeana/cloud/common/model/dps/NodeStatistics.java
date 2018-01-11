package eu.europeana.cloud.common.model.dps;

import java.util.ArrayList;
import java.util.List;

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
    private int occurrence;

    /** List of attributes together with their statistics */
    private List<AttributeStatistics> attributesStatistics = new ArrayList<>();

    public NodeStatistics(String parentXpath, String xpath, String value, int occurrence) {
        this(parentXpath, xpath, value, occurrence, new ArrayList<AttributeStatistics>());
    }

    public NodeStatistics(String parentXpath, String xpath, String value, int occurrence, List<AttributeStatistics> attributesStatistics) {
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

    public int getOccurrence() {
        return occurrence;
    }

    public List<AttributeStatistics> getAttributesStatistics() {
        return attributesStatistics;
    }

    public void setAttributesStatistics(List<AttributeStatistics> attributesStatistics) {
        this.attributesStatistics = attributesStatistics;
    }

    public boolean hasAttributes() {
        return !attributesStatistics.isEmpty();
    }
}
