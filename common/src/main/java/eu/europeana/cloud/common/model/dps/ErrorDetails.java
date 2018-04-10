package eu.europeana.cloud.common.model.dps;

import java.util.Objects;

/**
 * Class used for error reports. Stores a pair of identifier and additionalInfo.
 */
public class ErrorDetails {

    private String identifier;
    private String additionalInfo;

    /**
     * Default constructor
     */
    public ErrorDetails(){}

    /**
     * Constructor
     * @param identifier
     * @param additionalInfo
     */
    public ErrorDetails(String identifier, String additionalInfo) {
        this.identifier = identifier;
        this.additionalInfo = additionalInfo;
    }

    public String getAdditionalInfo() {
        return additionalInfo;
    }

    public void setAdditionalInfo(String additionalInfo) {
        this.additionalInfo = additionalInfo;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ErrorDetails that = (ErrorDetails) o;
        return Objects.equals(identifier, that.identifier) &&
                Objects.equals(additionalInfo, that.additionalInfo);
    }

    @Override
    public int hashCode() {

        return Objects.hash(identifier, additionalInfo);
    }

}
