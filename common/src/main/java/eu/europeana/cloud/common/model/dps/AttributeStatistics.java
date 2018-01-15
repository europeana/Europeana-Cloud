package eu.europeana.cloud.common.model.dps;

/**
 * Created by Tarek on 1/9/2018.
 */
public class AttributeStatistics {
    private String name;
    private String value;
    private int occurrence;

    public AttributeStatistics(String name, String value, int occurrence) {
        this.name = name;
        this.value = value;
        this.occurrence = occurrence;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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


    @Override
    public boolean equals(Object o) {

        if (o == this) return true;
        if (!(o instanceof AttributeStatistics)) {
            return false;
        }

        AttributeStatistics attributeModel = (AttributeStatistics) o;

        return attributeModel.getName().equals(name) &&
                attributeModel.getValue().equals(value);
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + name.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }
}
