package eu.europeana.cloud.common.model.dps;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Tarek on 1/9/2018.
 */
public class NodeStatistics {
    private String xpath;
    private String parentXpath;
    private String value;
    private int occurrence;
    private Set<AttributeStatistics> attributes;

    public NodeStatistics(String xpath, String parentXpath, String value, int occurrence) {
        this.xpath = xpath;
        this.parentXpath = parentXpath;
        this.value = value;
        this.occurrence = occurrence;
        attributes = new HashSet<>();
    }


    public String getParentXpath() {
        return parentXpath;
    }

    public void setParentXpath(String parentXpath) {
        this.parentXpath = parentXpath;
    }


    public Set<AttributeStatistics> getAttributes() {
        return attributes;
    }

    public void setAttributes(Set<AttributeStatistics> attributes) {
        this.attributes = attributes;
    }


    public String getXpath() {
        return xpath;
    }

    public void setXpath(String xpath) {
        this.xpath = xpath;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getOccurrence() {
        return occurrence;
    }

    public void setOccurrence(int occurrence) {
        this.occurrence = occurrence;
    }

    public void increaseOccurrence() {
        this.occurrence++;
    }


}
