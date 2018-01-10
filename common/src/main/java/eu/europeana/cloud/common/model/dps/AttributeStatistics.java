package eu.europeana.cloud.common.model.dps;

/**
 * Statistics of a node's attribute
 */
public class AttributeStatistics {
    /** Attribute name */
    private String name;

    /** Attribute value */
    private String value;

    /** Attribute value occurrence */
    private int occurrence;

    public AttributeStatistics(String name, String value) {
        this(name, value, 1);
    }

    public AttributeStatistics(String name, String value, int occurrence) {
        this.name = name;
        this.value = value;
        this.occurrence = occurrence <= 0 ? 1 : occurrence;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public int getOccurrence() {
        return occurrence;
    }

    public void increaseOccurrence(int count) {
        if (count > 0) {
            this.occurrence += count;
        }
    }
}
